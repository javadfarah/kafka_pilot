import asyncio
from contextlib import asynccontextmanager
from typing import Dict

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from kafka_pilot.db import redis_client
from kafka_pilot.utils import logger
from kafka_pilot.web_admin import setup_web_admin
from kafka_pilot.worker import KafkaWorker

# Global workers dictionary to manage running workers
workers: dict[str, KafkaWorker] = {}

console = Console(force_terminal=True, color_system="auto")


def print_initiation_text(tasks: Dict):
    console.print(Panel.fit(f"""
 █▄▀ ▄▀█ █▀▀ █▄▀ ▄▀█   █▀█ █ █   █▀█ ▀█▀
 █ █ █▀█ █▀  █ █ █▀█   █▀  █ █▄▄ █▄█  █ 
                             
""",
                            title="[yellow]🚀kafka pilot started[/yellow]",
                            border_style="bright_blue"
                            ))
    table = Table(title="[bold magenta]Registered Tasks[/bold magenta]", show_lines=True, header_style="bold magenta",
                  border_style="bright_blue")
    table.add_column("Topic", style="cyan", justify="left", overflow="fold")
    table.add_column("Processor class", style="green", overflow="fold")

    for topic, task_class in tasks.items():
        table.add_row(str(topic), str(task_class.__class__))

    console.print(table)
    console.print("Color support:", console.color_system)


async def kafka_pilot_startup():
    from kafka_pilot.processors import MessageProcessor
    processors_per_topic = {
        topic.strip(): {
            "processor": cls(),  # new instance per topic
            "worker_count": cls.instances  # class-level attribute
        }
        for cls in MessageProcessor.registry.values()
        for topic in cls().topic_conf.topics.split(',')
    }
    print(processors_per_topic)
    print_initiation_text(processors_per_topic)
    try:
        # Initialize Redis
        await redis_client.initialize()
        # Start workers for each topic
        for topic, data in processors_per_topic.items():
            processor = data["processor"]
            worker_count = data["worker_count"]

            workers[topic] = []  # store multiple workers per topic

            for i in range(worker_count):
                worker = KafkaWorker(processor=processor, topic=topic)  # optional id
                workers[topic].append(worker)
                asyncio.create_task(worker.start())
                logger.info(f"Started worker {i + 1}/{worker_count} for topic: {topic}")

    except Exception as e:
        logger.error(f"Error during startup: {e}", exc_info=True)

        # Graceful shutdown
        shutdown_tasks = [
            worker.stop()
            for worker_list in workers.values()
            for worker in worker_list  # flatten the nested list
        ]
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        await redis_client.close()
        logger.info("All workers and Redis stopped gracefully")


async def kafka_pilot_shutdown():
    # Graceful shutdown
    shutdown_tasks = [
        worker.stop()
        for worker_list in workers.values()
        for worker in worker_list
    ]

    await asyncio.gather(*shutdown_tasks, return_exceptions=True)
    await redis_client.close()
    logger.info("All workers and Redis stopped gracefully")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle."""

    await kafka_pilot_startup()
    yield
    await kafka_pilot_shutdown()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="kafka_pilot/templates/static"), name="static")
setup_web_admin(app, workers)  # Pass workers to web_admin


@app.get("/health")
async def health_check():
    """Health check endpoint for the application."""
    return {"status": "healthy"}


def main():
    """Run the FastAPI application with uvicorn."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
