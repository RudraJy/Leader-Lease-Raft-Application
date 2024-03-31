import threading
import time

def timer_callback():
    print("Timer expired!")

timer = threading.Timer(5, timer_callback)
timer.start()
time.sleep(2)
# Get the remaining time in seconds
remaining_time = timer.__doc__
# Print the remaining time
print("Remaining time:", remaining_time)

# Wait for the timer to expire
timer.join()