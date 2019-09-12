import databricks_cli.utils as utils
from concurrent.futures import CancelledError
import asyncio
import logging
from click.testing import CliRunner
from databricks_cli.dbfs import cli as dbfs_cli
from databricks_cli.jobs.api import AsyncJobsApi
from databricks_cli.jobs import cli as jobs_cli
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
import click
from httpx.exceptions import ReadTimeout

from databricks_cli.dbfs.dbfs_path import DbfsPath


from databricks_cli.configure.provider import DatabricksConfig, DEFAULT_SECTION, \
    update_and_persist_config


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class EventLoop(object):

    def __init__(self, api_client, num_concurrent_events):
        self.num_concurrent_events = num_concurrent_events
#        self.queue = None
        self.jobs_api = AsyncJobsApi(api_client)

    # async def setup_queue(self):
    #     if self.queue is None:
    #         self.queue = asyncio.Queue(maxsize=self.num_concurrent_events)
    #
    # async def _list_jobs(self, num_lists):
    #     await self.setup_queue()
    #     for i in range(num_lists):
    #         print(f"adding event list_jobs {i} ")
    #         await self.queue.put((i, self.jobs_api.list_jobs()))
    #     print("waiting for consume")
    #     await self.consume()
    #     print("waiting for join")
    #     await self.queue.join()
    #     print("queue has joined")
    #
    # async def consume(self):
    #     try:
    #         while True:
    #             (i, event) = await self.queue.get()
    #             print(f"Waiting for event {i}")
    #             result = await event
    #             print(f"event received", str(result)[:4])
    #             self.queue.task_done()
    #     except Exception as e:
    #         print(f"consume:{e}")

    async def list_one_call(self, i):
        print(f"list_one_call {i}: entered", flush=True)
        #logging.info("warning test: list_one_call")
        l = await self.jobs_api.list_jobs()
        print(f"list_one_call {i}: finished: {str(l)[:4]}")
        return l

    # async def test(self, num_lists):
    #     print(f"test: entered")
    #     tasks = [self.list_one_call(i) for i in range(num_lists)]
    #     await asyncio.gather(*tasks)
    #     print(f"test: finished")
    #
    # def list_jobs(self, num_lists):
    #     try:
    #         asyncio.run(self.test(num_lists), debug=True)
    #     except Exception as e:
    #         print(f"list_jobs: exception: {e}")

    def list_jobs_2(self, num_lists, num_concurrent_tasks):
        try:
            asyncio.run(self.test_2(num_lists, num_concurrent_tasks), debug=True)
        except Exception as e:
            print(f"list_jobs_2: exception: {type(e)} {e}")
            raise e

    async def test_2(self, num_calls, num_concurrent_tasks):
        logging.warning("warning test")
        queue = asyncio.Queue(maxsize=3 * num_concurrent_tasks)
        # schedule the consumer
        consumers = [asyncio.ensure_future(self.consume(queue)) for _ in range(num_concurrent_tasks)]
        # run the producer and wait for completion
        await self.produce(queue, num_calls)
        # wait until the consumer has processed all items
        await queue.join()
        # the consumer is still awaiting for an item, cancel it
        for c in consumers:
            c.cancel()

    async def produce(self, queue, num_calls):
        for call_idx in range(num_calls):
            try:
                print(f"produce: adding call {call_idx}", flush=True)
                # put the item in the queue
                await queue.put(call_idx)
                await asyncio.sleep(0.001)
            except Exception as e:
                print(f"produce: exception call_idx={call_idx} type={type(e)} e={e}")
                if isinstance(e, (RuntimeError, CancelledError)):
                    raise e

    async def consume(self, queue):
        while True:
            call_idx = None
            try:
                # wait for an item from the producer
                call_idx = await queue.get()
                print('consuming {}...'.format(call_idx), flush=True)
                await asyncio.sleep(0.0001)
                res = await self.list_one_call(call_idx)
                await asyncio.sleep(0.0001)
                item = str(res)[:10]
                logging.info(f"consume: item {call_idx}: res={item}")

                # process the item
                # simulate i/o operation using sleep
                #await asyncio.sleep(random.random())

                # Notify the queue that the item has been processed
                queue.task_done()
            except Exception as e:
                print(f"produce: exception call_idx={call_idx} type={type(e)} e={e}")
                if isinstance(e, (RuntimeError, CancelledError)):
                    raise e




@click.command(context_settings=utils.CONTEXT_SETTINGS,
               short_help='Lists the jobs in the Databricks Job Service.')
@click.option('--output', default=None, help="no help", type=int)
@debug_option
@profile_option
@provide_api_client
def main_stresstest(api_client, output):
    """
    Lists the jobs in the Databricks Job Service.

    By default the output format will be a human readable table with the following fields

      - Job ID

      - Job name

    A JSON formatted output can also be requested by setting the --output parameter to "JSON"

    In table mode, the jobs are sorted by their name.
    """
    print("creating main event loop")
    el = EventLoop(api_client, 2)
    print("list_jobs")
    el.list_jobs_2(2000, 500)
    print("list_jobs:done")
    # jobs_api = AsyncJobsApi(api_client)
    # jobs_json = jobs_api.list_jobs()
    # print(jobs_json)
    # loop = asyncio.get_event_loop()
    # res = loop.run_until_complete(jobs_json)
    # loop.close()
    # print("event", res)


if __name__ == '__main__':
    print("xxx")
    runner = CliRunner()
    res = runner.invoke(main_stresstest, [])
    print(res.output)
