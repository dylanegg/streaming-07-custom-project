"""
   This program listents for messages sent at the end of the period of hockey games. 
   Then, end of period score alerts are given to the user. 

    Author: Dylan Eggemeyer
    Date: February 20, 2023

"""

import pika
import sys
import time

# define a callback function to be called when a message is received at the end of the period
def scores_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Received {body.decode()} on hockey_queue")
    # acknowledge the message was received and processed 
    ch.basic_ack(delivery_tag=method.delivery_tag)
    # convert message from binary to tuple
    message = body.decode().split(",")
    # Define the variables from the body
    period = str(message[0])
    team1 = message[1]
    team1_score = message[2]
    team2 = message[3]
    team2_score = message[4]
    # create message of end of period score or send overtime alert
    if period == "3" and team1_score == team2_score:
        print(f"Overtime Alert! \n{team1} {team1_score} : {team2_score} {team2}") #alert when game is going to overtime
    else:
        print(f"End of Period {period}. \n{team1} {team1_score} : {team2_score} {team2}") #alert when period is ended



# define a main function to run the program for 3 queues
def main(hn: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queues
        channel.queue_declare(queue="hockey_queue", durable=True)

        # set the prefetch count    
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        channel.basic_consume( queue="hockey_queue", on_message_callback=scores_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")
