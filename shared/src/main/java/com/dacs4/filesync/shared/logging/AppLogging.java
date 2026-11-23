package com.dacs4.filesync.shared.logging;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/** Console configuration for the JDK System.Logger backend. */
public final class AppLogging {
    private static final String NAMESPACE = "com.dacs4.filesync";
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
        .withZone(ZoneId.systemDefault());
    private static final Logger PROJECT = Logger.getLogger(NAMESPACE);
    private static final Pattern CONTROL_CHARACTERS = Pattern.compile("[\\p{Cc}\\p{Zl}\\p{Zp}]");
    private static ConsoleHandler console;

    private AppLogging() {}

    public static synchronized void configure() {
        if (console != null) {
            return;
        }
        Level level = switch (System.getProperty("filesync.log.level", "INFO").strip().toUpperCase(Locale.ROOT)) {
            case "TRACE" -> Level.FINER;
            case "DEBUG" -> Level.FINE;
            case "WARN", "WARNING" -> Level.WARNING;
            case "ERROR" -> Level.SEVERE;
            default -> Level.INFO;
        };
        console = new ConsoleHandler();
        console.setLevel(level);
        console.setFormatter(new Formatter() {
            @Override public String format(LogRecord record) {
                StringBuilder line = new StringBuilder().append(TIME.format(record.getInstant()))
                    .append(" [").append(severity(record.getLevel())).append("] [")
                    .append(component(record.getLoggerName())).append("] ")
                    .append(CONTROL_CHARACTERS.matcher(formatMessage(record)).replaceAll(" "));
                Throwable problem = record.getThrown();
                for (int depth = 0; problem != null && depth < 3; depth++, problem = problem.getCause()) {
                    line.append(depth == 0 ? " (" : " <- ").append(problem.getClass().getSimpleName());
                    for (StackTraceElement frame : problem.getStackTrace()) {
                        if (frame.getClassName().startsWith(NAMESPACE + ".")) {
                            line.append(" at ").append(frame.getFileName()).append(':').append(frame.getLineNumber());
                            break;
                        }
                    }
                    if (problem.getSuppressed().length != 0) {
                        line.append(", suppressed=").append(problem.getSuppressed().length);
                    }
                }
                if (record.getThrown() != null) {
                    line.append(')');
                }
                return line.append(System.lineSeparator()).toString();
            }
        });
        PROJECT.setUseParentHandlers(false);
        PROJECT.setLevel(level);
        PROJECT.addHandler(console);
    }

    /** The JVM may detach JUL handlers before application shutdown hooks finish. */
    public static synchronized void logShutdown(Class<?> source, String message) {
        if (console != null) {
            var record = new LogRecord(Level.INFO, message);
            record.setLoggerName(source.getName());
            console.publish(record);
        }
    }

    private static String severity(Level level) {
        return switch (level.getName()) {
            case "SEVERE" -> "ERROR";
            case "WARNING" -> "WARN";
            case "FINE" -> "DEBUG";
            case "FINER", "FINEST" -> "TRACE";
            default -> level.getName();
        };
    }

    private static String component(String logger) {
        String name = logger.substring(logger.lastIndexOf('.') + 1);
        return switch (name) {
            case "ServerMain", "TcpServer", "ClientHandler" -> "Server";
            case "Main", "ConnectionService", "TcpClient" -> "Client";
            case "SyncTransferManager", "SyncCoordinator", "LocalFolderWatcher", "FileNotificationService" -> "Sync";
            case "ConflictRegistry" -> "Conflict";
            case "FileTransferService", "FileTransferClient", "FileService" -> "Transfer";
            case "FileStorageService", "VersionStore" -> "Archive";
            case "Restore" -> "Restore";
            case "ConflictComparison" -> "Compare";
            default -> name;
        };
    }

    /** Independent limiter for a single repetitive warning source. */
    public static final class WarningLimiter {
        private static final long INTERVAL = TimeUnit.SECONDS.toNanos(30);
        private final AtomicLong last = new AtomicLong(Long.MIN_VALUE);

        public boolean allow() {
            long now = System.nanoTime();
            long previous = last.get();
            return (previous == Long.MIN_VALUE || now - previous >= INTERVAL) && last.compareAndSet(previous, now);
        }
    }
}
