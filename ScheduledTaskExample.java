import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.time.Duration;

public class ScheduledTaskExample {
    public static void main(String[] args) {
        // Specific time to run the task (e.g., 2023-07-09 14:30:00)
        LocalDateTime specificTime = LocalDateTime.of(2023, 7, 9, 14, 30, 0);
        
        // Calculate the delay in seconds
        LocalDateTime now = LocalDateTime.now();
        long delayInSeconds = Duration.between(now, specificTime).getSeconds();

        // Create a ScheduledExecutorService
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Schedule the task
        scheduler.schedule(() -> {
            // Task to execute
            System.out.println("Task executed at: " + LocalDateTime.now());
        }, delayInSeconds, TimeUnit.SECONDS);

        // Shutdown the scheduler after the task is executed
        scheduler.schedule(() -> {
            scheduler.shutdown();
        }, delayInSeconds + 1, TimeUnit.SECONDS); // +1 second to ensure the task is completed
    }
}
