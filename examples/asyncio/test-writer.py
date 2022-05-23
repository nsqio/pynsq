from tornado.platform.asyncio import AsyncIOMainLoop
import nsq, asyncio

def finish_pub(conn, data):
    print(data)
    print(conn)

def nsq_reader():
    pass    


async def push_to_nsq():
    producer = nsq.Writer(nsqd_tcp_addresses=['localhost:4150'])
    await asyncio.sleep(1) # very need or SendError: no open connections (None)
    
    producer.pub("nsq-test-writer", "test".encode()*100*1000, finish_pub)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    AsyncIOMainLoop().install()
    loop.create_task(push_to_nsq())
    loop.run_forever()
