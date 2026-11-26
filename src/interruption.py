import requests
import threading
import time

IMDS_BASE_URL = "http://169.254.169.254"


def get_metadata_token():
    response = requests.put(
        f"{IMDS_BASE_URL}/latest/api/token",
        headers={
            "X-aws-ec2-metadata-token-ttl-seconds": "21600"
        },
        timeout=2
    )

    response.raise_for_status()

    return response.text

def get_interruption_notice(token):
    response = requests.get(
        f"{IMDS_BASE_URL}/latest/meta-data/spot/instance-action",
        headers={
            "X-aws-ec2-metadata-token": token
        },
        timeout=2
    )

    if response.status_code == 404:
        return None

    response.raise_for_status()

    return response.json()

stop_event = threading.Event()


def monitor_interruption():
    token = None

    while not stop_event.is_set():
        try:
            if token is None:
                token = get_metadata_token()

            notice = get_interruption_notice(token)

            if notice is not None:
                print(
                    f"Spot interruption detected: {notice}",
                    flush=True
                )

                stop_event.set()
                return

        except requests.RequestException as error:
            print(f"IMDS request failed: {error}", flush=True)
            token = None

        stop_event.wait(5)

def start_interruption_monitor():
    monitor_thread = threading.Thread(
        target=monitor_interruption,
        daemon=True,
        name="spot-interruption-monitor"
    )

    monitor_thread.start()

    return monitor_thread