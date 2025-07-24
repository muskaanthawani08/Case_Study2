import logging

def send_alert():
    logging.warning("ALERT: Data quality validation failed. Please check pipeline output.")
