from celery import shared_task

task_registry = {}


def handle_topic(topic: str):
    def wrapper(func):
        task_registry[func] = topic
        return func

    return wrapper


@handle_topic("*.created")
@shared_task(name="something_created")
def something_created(x: str):
    print(f"Something created: {x}")


@handle_topic("dataset.created")
@shared_task(name="dataset_created")
def dataset_created(x: str):
    print(f"Dataset created: {x}")
