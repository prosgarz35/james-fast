import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SmtpHighLoadBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 25;
    private static final String FROM = "alice@testcorp.local";
    private static final String TO = "bob@testcorp.local";

    public static void main(String[] args) throws Exception {
        int totalMessages = args.length > 0 ? Integer.parseInt(args[0]) : 10000;
        int concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 0;

        if (concurrency <= 0) {
            concurrency = detectSpoolerThreads();
        }

        System.out.println("=== Starting SmtpHighLoadBenchmark ===");
        System.out.println("Target: " + HOST + ":" + PORT);
        System.out.println("Total messages: " + totalMessages);
        System.out.println("Concurrency: " + concurrency + " threads (synchronized with James spooler)");

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        AtomicInteger sentSuccess = new AtomicInteger(0);
        AtomicInteger sentErrors = new AtomicInteger(0);
        AtomicLong totalLatencyMs = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        int perThread = totalMessages / concurrency;
        int remainder = totalMessages % concurrency;

        CountDownLatch latch = new CountDownLatch(concurrency);

        for (int t = 0; t < concurrency; t++) {
            final int threadId = t;
            final int count = perThread + (t == concurrency - 1 ? remainder : 0);

            executor.submit(() -> {
                try {
                    // Reusing keep-alive SMTP session per thread where possible, or reconnecting
                    Socket socket = null;
                    BufferedReader reader = null;
                    BufferedWriter writer = null;

                    int sessionMsgCount = 0;

                    for (int i = 0; i < count; i++) {
                        long msgStart = System.currentTimeMillis();
                        try {
                            if (socket == null || socket.isClosed() || sessionMsgCount >= 500) {
                                if (socket != null) {
                                    try {
                                        writer.write("QUIT\r\n");
                                        writer.flush();
                                        socket.close();
                                    } catch (Exception ignored) {}
                                }
                                socket = new Socket(HOST, PORT);
                                socket.setTcpNoDelay(true);
                                socket.setSoTimeout(15000);
                                reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII), 8192);
                                writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.US_ASCII), 8192);
                                String banner = reader.readLine();
                                writer.write("HELO benchmark\r\n");
                                writer.flush();
                                String heloRes = reader.readLine();
                                sessionMsgCount = 0;
                            }

                            writer.write("MAIL FROM:<" + FROM + ">\r\n");
                            writer.write("RCPT TO:<" + TO + ">\r\n");
                            writer.write("DATA\r\n");
                            writer.flush();

                            String mailRes = reader.readLine();
                            String rcptRes = reader.readLine();
                            String dataRes = reader.readLine();

                            if (dataRes == null || !dataRes.startsWith("354")) {
                                throw new IOException("DATA command rejected: " + dataRes);
                            }

                            // Write headers and small body (~300 bytes)
                            writer.write("From: <" + FROM + ">\r\n");
                            writer.write("To: <" + TO + ">\r\n");
                            writer.write("Subject: HighLoad Bench Msg " + threadId + "-" + i + "\r\n");
                            writer.write("Message-ID: <" + threadId + "." + i + "." + System.nanoTime() + "@benchmark>\r\n");
                            writer.write("MIME-Version: 1.0\r\n");
                            writer.write("Content-Type: text/plain; charset=utf-8\r\n");
                            writer.write("\r\n");
                            writer.write("High load benchmark payload small size test message: " + i + "\r\n");
                            writer.write(".\r\n");
                            writer.flush();

                            String finishRes = reader.readLine();
                            if (finishRes != null && finishRes.startsWith("250")) {
                                sentSuccess.incrementAndGet();
                                sessionMsgCount++;
                            } else {
                                sentErrors.incrementAndGet();
                                // reconnect on protocol error
                                socket.close();
                                socket = null;
                            }

                            long lat = System.currentTimeMillis() - msgStart;
                            totalLatencyMs.addAndGet(lat);

                        } catch (Exception ex) {
                            sentErrors.incrementAndGet();
                            if (socket != null) {
                                try { socket.close(); } catch (Exception ignored) {}
                                socket = null;
                            }
                        }

                        int currentSuccess = sentSuccess.get();
                        if (currentSuccess > 0 && currentSuccess % 2000 == 0) {
                            long elapsed = System.currentTimeMillis() - startTime;
                            double rps = currentSuccess / (elapsed / 1000.0);
                            System.out.printf("Progress: %d / %d sent (%.1f msgs/sec, errors: %d)%n",
                                    currentSuccess, totalMessages, rps, sentErrors.get());
                        }
                    }

                    if (socket != null) {
                        try {
                            writer.write("QUIT\r\n");
                            writer.flush();
                            socket.close();
                        } catch (Exception ignored) {}
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long durationMs = System.currentTimeMillis() - startTime;
        double totalSeconds = durationMs / 1000.0;
        int success = sentSuccess.get();
        int errors = sentErrors.get();
        double avgRps = success / totalSeconds;
        double avgLatency = success > 0 ? (totalLatencyMs.get() / (double) success) : 0;

        System.out.println("\n=== High Load Benchmark Results ===");
        System.out.println("Total Time: " + totalSeconds + " s");
        System.out.println("Successfully Delivered: " + success + " / " + totalMessages + " (" + String.format("%.2f", (success * 100.0 / totalMessages)) + "%)");
        System.out.println("Errors / Dropped: " + errors);
        System.out.println("Average Throughput: " + String.format("%.1f", avgRps) + " msgs/sec");
        System.out.println("Average Latency: " + String.format("%.2f", avgLatency) + " ms");
    }

    private static int detectSpoolerThreads() {
        File[] candidates = new File[] {
            new File("../james/conf/mailetcontainer.xml"),
            new File("james/conf/mailetcontainer.xml"),
            new File("C:/soft/james_next_2/james/conf/mailetcontainer.xml")
        };
        for (File f : candidates) {
            if (f.exists()) {
                try {
                    String content = new String(java.nio.file.Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
                    java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("<threads>(\\d+)</threads>").matcher(content);
                    if (matcher.find()) {
                        int threads = Integer.parseInt(matcher.group(1));
                        if (threads > 0) {
                            return threads;
                        }
                    }
                } catch (Exception ignored) {}
            }
        }
        return 16;
    }
}
