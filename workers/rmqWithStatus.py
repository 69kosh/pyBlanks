import json
from multiprocessing import Event, current_process, Process, freeze_support
from threading import Thread
from signal import signal, SIGTERM, SIGINT
import time
from pika import BlockingConnection, URLParameters, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from functools import partial

from .messages import BaseMessage, StatusMessage, StatusMessageType

_rmqProcesses = []
_stopEvent = Event()
_rmqConnections = {}


def connectToRmq(url) -> BlockingConnection:
	global _rmqConnections
	if url not in _rmqConnections:
		_rmqConnections[url] = BlockingConnection(URLParameters(url))

	return _rmqConnections[url]


def stopConsuming(connection: BlockingConnection, channel: BlockingChannel, stopEvent: Event):
	process = current_process().name
	print(' [*]', process, 'Waiting for stop event')
	stopEvent.wait()
	print(' [*]', process, 'Stopping channel')
	channel.stop_consuming()
	# connection.close()
	print(' [*]', process, 'Channel stopped, connection closed')


def startConsuming(url: str, queue: str, worker: callable, stopEvent: Event):  # , stopEvent

	process = current_process().name
	print(' [*]', process, 'Consumer process started with worker',
		  worker, 'on queue', queue, flush=True)
	connection = connectToRmq(url)
	channel = connection.channel()
	print(' [*]', process, 'Connected to RMQ', flush=True)
	channel.queue_declare(queue=queue, durable=True)
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(on_message_callback=worker,
						  queue=queue)  # , auto_ack=True

	print(' [*]', process, 'Waiting for messages', flush=True)

	Thread(target=stopConsuming, args=(connection, channel, stopEvent)).start()
	try:
		channel.start_consuming()
	except KeyboardInterrupt:
		pass

	print(' [x]', process, 'Consumer process finished', flush=True)


def term(*args, **kwargs):
	raise KeyboardInterrupt


def publishMessage(url, exchange, route, message: BaseMessage):
	json = message.json()
	connection = connectToRmq(url)
	channel = connection.channel()
	channel.basic_publish(exchange=exchange,
						  routing_key=route,
						  body=json,
						  properties=BasicProperties(content_encoding='utf-8', content_type='application/json'))
	# print(" [x] Sent message", message)


def consumerWorker(ch: BlockingChannel, method, properties, body,
				   job: callable,
				   messageClass: BaseMessage,
				   resultUrl: str,
				   resultExchange: str,
				   resultRoute: str,
				   resultClass: BaseMessage,
				   statusUrl: str,
				   statusExchange: str,
				   statusRoute: str,
				   statusClass: StatusMessage):

	process = current_process().name

	try:
		message = messageClass.parse_obj(json.loads(body))
	except Exception as e:
		print(' [x]', process, 'Error', e.__class__,
			  e, 'Raw message:\n', body, flush=True)
		statusMessage = statusClass(id='Unknown',
									type=StatusMessageType.FAILURE,
									description='Body parsing error: ' +
									str(e.__class__) + ' - ' + str(e),
									raw=body)
		publishMessage(statusUrl, statusExchange, statusRoute, statusMessage)
		ch.basic_ack(delivery_tag=method.delivery_tag)
		return

	try:
		# create and send start status message and ack
		statusMessage = statusClass(
			id=message.id, type=StatusMessageType.STARTED)
		publishMessage(statusUrl, statusExchange, statusRoute, statusMessage)
		ch.basic_ack(delivery_tag=method.delivery_tag)
		print(' [x]', process, 'Started', message.id, flush=True)
		outMessage = None
		for outMessage in job(message=message):
			if isinstance(outMessage, resultClass):
				# send result
				# print(' [x]', process, 'Result message',
				# 	  outMessage.id, flush=True)
				publishMessage(resultUrl, resultExchange,
							   resultRoute, outMessage)
				# pass
			if isinstance(outMessage, statusClass):
				# send status message
				# print(' [x]', process, 'Status message', outMessage.id,
				# 	  outMessage.type, outMessage.progress, flush=True)
				publishMessage(statusUrl, statusExchange,
							   statusRoute, outMessage)
				# pass

			print(' [x]', process, 'Returned', message.id, 'type', type(outMessage), flush=True)

		if isinstance(outMessage, resultClass):
			# last message is result - send status success
			statusMessage = statusClass(id=message.id,
										type=StatusMessageType.SUCCESS,
										progress=1.0)
			publishMessage(statusUrl, statusExchange, statusRoute, statusMessage)
			print(' [x]', process, 'Success', message.id, flush=True)
	except Exception as e:
		print(' [x]', process, 'Error', e.__class__,
			  e, 'Raw message:\n', body, flush=True)
		# create and send status message
		statusMessage = statusClass(id=message.id,
									type=StatusMessageType.FAILURE,
									description='Error: ' +
									str(e.__class__) + ' - ' + str(e),
									raw=body)
		publishMessage(statusUrl, statusExchange, statusRoute, statusMessage)


def runWorkers(messageClass: BaseMessage,
			   resultUrl: str,
			   resultExchange: str,
			   resultRoute: str,
			   resultClass: BaseMessage,
			   statusUrl: str,
			   statusExchange: str,
			   statusRoute: str,
			   statusClass: BaseMessage,
			   url: str, queue: str, job: callable,
			   workers: int = 4, name: str = None):

	global _rmqProcesses
	global _stopEvent
	freeze_support()
	signal(SIGTERM, term)
	signal(SIGINT, term)

	wrappedWorker = partial(consumerWorker, job=job,
							messageClass=messageClass,
							resultUrl=resultUrl,
							resultExchange=resultExchange,
							resultRoute=resultRoute,
							resultClass=resultClass,
							statusUrl=statusUrl,
							statusExchange=statusExchange,
							statusRoute=statusRoute,
							statusClass=statusClass)

	currentName = name
	for i in list(range(1, workers+1)):
		if name is not None:
			currentName = name + '-' + str(i)
		process = Process(name=currentName, target=startConsuming, kwargs={
			'url': url,
			'queue': queue,
			'worker': wrappedWorker,
			'stopEvent': _stopEvent
		})
		_rmqProcesses.append(process)
		process.start()
	print(' [*] Loop started')


def waitWorkers():
	global _rmqProcesses
	global _stopEvent
	try:
		while True:
			# print(' [.] Stay alive...')
			time.sleep(1)

	except KeyboardInterrupt:
		print(' [x] Shutting down...')
		_stopEvent.set()
		for process in _rmqProcesses:
			process.join()

	print(' [x] Shutdown')


if __name__ == '__main__':
	pass
