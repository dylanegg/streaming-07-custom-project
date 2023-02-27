"""
    This program sends a message to a queue.
    The csv used in this program simulates end of period scores for a night of hockey in the NHL.
    In real life, these messages would be sent at the end of each period. For the sake of this project, messages are sent at 30 second intervals.

    Author: Dylan Eggemeyer
    Date: February 20, 2023

"""
import pika
import sys
import webbrowser
import csv
import time

# Define Global Variables

# decide if you want to show the offer to open RabbitMQ admin site
# Input "True" or "False"
show_offer = "False"

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to delete the queue
        ch.queue_delete(queue="hockey_queue")
        # use the channel to declare the durable queue
        ch.queue_declare(queue="hockey_queue", durable=True)

        # open the input file
        input_file = open("hockey_scores.csv", "r")
        # read the input file
        reader = csv.reader(input_file,delimiter=",")
        # skip the headers
        next(reader, None)
        # get message from each row of file
        for row in reader:
            # read a row from the file
            Period, Team1, Team1_Score, Team2, Team2_Score  = row 
            
            # use f string to create message from data
            f_message = f"{Period}, {Team1}, {Team1_Score}, {Team2}, {Team2_Score}"

            # Serialize the message into binary data
            message = f_message.encode()

            # send the message to an individual queue
            ch.basic_publish(exchange="", routing_key="hockey_queue", body=message)

            # print a message to the console for the user
            print(f" [x] Sent {message}")

            # wait 30 seconds before sending next score
            time.sleep(30)
        # close the file
        input_file.close()
        
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # show the offer to open Admin Site if show_offer is set to true, else open automatically
    if show_offer == "True":
        offer_rabbitmq_admin_site()
    else:
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()
    # Use the send_message function to start the stream
    send_message("localhost")
    