#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend
#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from random import sample
from logging import basicConfig, WARNING, warning

# DATABASE
from redis import StrictRedis
from MySQLdb import escape_string

# TELEGRAM
from telegram import Emoji, ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardHide, TelegramError
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters

# SERIALIZING
from msgpack import packb, unpackb

# SELF IMPORT
from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, TELEGRAM_BOT_TOKEN
from utilities import exec_mysql, get_mysql_con, refresh_mysql_con, bot_track
from event import EventLight

basicConfig(filename='telegram.log', level=WARNING, format=u'[%(asctime)s] LINE: #%(lineno)d | %(levelname)-8s | %(message)s')
#basicConfig(level=WARNING, format=u'[%(asctime)s] LINE: #%(lineno)d | %(levelname)-8s | %(message)s')

CONTEXT = {}

TEXTS = {
'help.regular':"""Welcome to our bot-newsroom. Our media is fully controlled by bots. Journalists-bots scan social media, and look for newsbreaks. Editor-bot selects events, that you may be interested in. And I'm, the anchor-bot from Telegram, broadcasting news to you.

You can control me by sending these commands:

/now - Current event list
/trending - Top recent events""",

'help.admin':"""Welcome to our bot-newsroom. Our media is fully controlled by bots. Journalists-bots scan social media, and look for newsbreaks. Editor-bot selects events, that you may be interested in. And I'm, the anchor-bot from Telegram, broadcasting news to you.

You can control me by sending these commands:

/now - Current event list
/trending - Top recent events
/teach - Training Editor-bot to distinguish real events from white noise

Send me a contact to grant this user additional priveledges.
""",

'teach.start':"""That's great! Let's improve my skill of event detecting. I will show you random social media messages clusters, and you should answer, if it is a real newsbreak. Got tired? Press "Break".""",

'teach.placeholder':"""Wait, while we are loading an event...""",

'teach.nodata':"""Unfortunately, there is nothing to classify yet. Come back later.""",

'teach.finish':"""Ok. That's enough for now. Let's have a break.""",

'unknown.command':"""Strange answer...""",

'ok':"""Ok!""",

'no.context':"""What we were talking about?""",

'contact.new':"""You sent me a contact. If you want to add this user to a privileged group, specify it, or press "Cancel".""",

'contact.added':u"""Added {} to group {}""",

}

updater = Updater(token=TELEGRAM_BOT_TOKEN)
dispatcher = updater.dispatcher
redis_con = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
mysql_con = get_mysql_con()

def role_dispatcher(role=None):
	def decorator(func):
		def wrapper(bot, update):
			users = {'admin':redis_con.smembers("admins"), 'tester':redis_con.smembers("testers")}
			if role and role in users.keys() and str(update.message.from_user.id) in users[role]:
				return func(bot, update)
			elif not role:
				if str(update.message.from_user.id) in users['admin']:
					return func(bot, update, role='admin')
				elif str(update.message.from_user.id) in users['tester']:
					return func(bot, update, role='tester')
				else:
					return func(bot, update, role='other')
		return wrapper
	return decorator

def send_stat(func):
	def wrapper(bot, update):
		if getattr(update, 'message', None):
			user = update.message.from_user
			event_dict = update.message.to_dict()
			action = update.message.text
		elif getattr(update, 'callback_query', None):
			user = update.callback_query.from_user
			event_dict = update.callback_query.to_dict()
			action = update.callback_query.data
		else:
			user = 'unknown'
			event_dict = {}
			action = 'unknown'
		bot_track(user, event_dict, action)
		return func(bot, update)
	return wrapper

@send_stat
@role_dispatcher()
def start_command(bot, update, role):
	if role == 'admin':
		bot.sendMessage(chat_id=update.message.chat_id, text=TEXTS['help.admin'])
	elif role == 'tester':
		bot.sendMessage(chat_id=update.message.chat_id, text=TEXTS['help.regular'])

@send_stat
@role_dispatcher()
def help_command(bot, update, role):
	if role=='admin':
		bot.sendMessage(chat_id=update.message.chat_id, text=TEXTS['help.admin'])
	else:
		bot.sendMessage(chat_id=update.message.chat_id, text=TEXTS['help.regular'])

@role_dispatcher("admin")
def teach_command(bot, update):
	global CONTEXT
	global mysql_con
	mysql_con = refresh_mysql_con(mysql_con)
	bot.sendMessage(chat_id=update.message.chat_id, text=TEXTS['teach.start'], reply_markup=ReplyKeyboardHide())
	msg = bot.sendMessage(chat_id=update.message.chat_id, text=TEXTS['teach.placeholder'])
	CONTEXT[str(update.message.from_user.id)] = {'command':'teach', 'chat':msg.chat_id, 'message':msg.message_id}
	publish_event(bot, str(update.message.from_user.id))

@send_stat
def callback_dispatcher(bot, update):
	global CONTEXT
	global redis_con
	query = update.callback_query
	user = str(query.from_user.id)
	verific_dict = {'teach.real':1, 'teach.fake':0}
	if user in CONTEXT.keys():
		if CONTEXT[user]['command'] == 'teach':
			q = 'SELECT dumps FROM events WHERE id = "{}";'.format(CONTEXT[user]['event'].id)
			data = unpackb(exec_mysql(q, mysql_con)[0][0]['dumps'])
			if query.data in ['teach.real', 'teach.fake']:
				data['verification'] = int(verific_dict[query.data])
				data = packb(data)
				q = b'''UPDATE events SET dumps = "{}", verification = {} WHERE id = "{}";'''.format(escape_string(data), verific_dict[query.data], CONTEXT[user]['event'].id)
				exec_mysql(q, mysql_con)
				del CONTEXT[user]['event']
				del CONTEXT[user]['event_limits']
				bot.answerCallbackQuery(query.id, text=TEXTS['ok'])
				publish_event(bot, user)
			elif query.data == 'teach.prev_msgs':
				publish_event(bot, user, CONTEXT[user]['event_limits'][0]-1, False)
			elif query.data == 'teach.next_msgs':
				publish_event(bot, user, CONTEXT[user]['event_limits'][1]+1, True)
			elif query.data == 'teach.finish':
				bot.editMessageText(text=TEXTS['teach.finish'], chat_id=CONTEXT[user]['chat'], message_id=CONTEXT[user]['message'])
				del CONTEXT[user]
			else:
				bot.answerCallbackQuery(query.id, text=TEXTS['unknown.command'])
		elif CONTEXT[user]['command'] == 'adduser':
			if query.data == 'adduser.cancel':
				del CONTEXT[user]
			elif query.data in ('adduser.admin', 'adduser.tester'):
				group = query.data[8:]+'s'
				redis_con.sadd(group, CONTEXT[user]['userid'])
				bot.editMessageText(text=TEXTS['contact.added'].format(CONTEXT[user]['username'], group), chat_id=CONTEXT[user]['chat'], message_id=CONTEXT[user]['message'])
			else:
				bot.answerCallbackQuery(query.id, text=TEXTS['unknown.command'])
		else:
			bot.answerCallbackQuery(query.id, text=TEXTS['unknown.command'])
	else:
		bot.answerCallbackQuery(query.id, text=TEXTS['no.context'])

def publish_event(bot, user, from_msg=0, direction=True):
	global CONTEXT
	keyboard = InlineKeyboardMarkup(
		[[InlineKeyboardButton(Emoji.WHITE_HEAVY_CHECK_MARK.decode('utf-8')+' Real', callback_data='teach.real'),
		InlineKeyboardButton(Emoji.CROSS_MARK.decode('utf-8')+' Fake', callback_data='teach.fake'),
		InlineKeyboardButton(Emoji.BLACK_LEFT_POINTING_TRIANGLE.decode('utf-8')+' Prev', callback_data='teach.prev_msgs'),
		InlineKeyboardButton(Emoji.BLACK_RIGHT_POINTING_TRIANGLE.decode('utf-8')+' Next', callback_data='teach.next_msgs'),
		InlineKeyboardButton(Emoji.BACK_WITH_LEFTWARDS_ARROW_ABOVE.decode('utf-8')+' Break', callback_data='teach.finish'),
		]])
	if 'event' not in CONTEXT[user].keys():
		try:
			event = get_random_event()
		except ValueError:
			bot.sendMessage(text=TEXTS['teach.nodata'], chat_id=CONTEXT[user]['chat'], reply_markup=ReplyKeyboardHide())
			del CONTEXT[user]
			return
		else:
			CONTEXT[user]['event'] = event
			CONTEXT[user]['event_limits'] = (0,0)

	to_publish, CONTEXT[user]['event_limits'] = CONTEXT[user]['event'].telegram_representation(from_msg, direction)
	try:
		bot.editMessageText(text=to_publish.decode('utf-8', errors='replace'), chat_id=CONTEXT[user]['chat'], message_id=CONTEXT[user]['message'], reply_markup=keyboard, parse_mode="Markdown")
	except TelegramError as e:
		if str(e) == 'Bad Request: message is not modified (400)':
			pass
		else:
			print e
			print to_publish
			del CONTEXT[user]

@send_stat
def adduser_command(bot, update):
	global CONTEXT
	contact = update.message.contact
	keyboard = InlineKeyboardMarkup(
		[[InlineKeyboardButton(Emoji.SMILING_FACE_WITH_HORNS.decode('utf-8')+' Admin', callback_data='adduser.admin'),
		InlineKeyboardButton(Emoji.MONKEY_FACE.decode('utf-8')+' Tester', callback_data='adduser.tester'),
		InlineKeyboardButton(Emoji.BACK_WITH_LEFTWARDS_ARROW_ABOVE.decode('utf-8')+' Cancel', callback_data='adduser.cancel'),
		]])
	msg = bot.sendMessage(text=TEXTS['contact.new'], chat_id=update.message.chat_id, reply_markup=keyboard)
	CONTEXT[str(update.message.from_user.id)] = {'command':'adduser', 'chat':msg.chat_id, 'message':msg.message_id, 'userid':contact['user_id'], 'username':' '.join([contact['first_name'], contact['last_name']]).strip()}

def get_random_event():
	q = 'SELECT * FROM events WHERE verification IS NULL AND description != "" ORDER BY end DESC LIMIT 5;'
	data = exec_mysql(q, mysql_con)[0]
	data = sample(data,1)[0]
	event = EventLight(start=data['start'], end=data['end'], validity=data['validity'], description=data['description'], dump=data['dumps'], mysql_con=mysql_con)
	return event

"""
def test_command(bot, update, *args):
	print args
	msg = bot.sendMessage(chat_id=update.message.chat_id, text=TEXTS['teach.placeholder'])
	try:
		bot.editMessageText(text=TEXTS['teach.placeholder'], chat_id=msg.chat_id, message_id=msg.message_id, parse_mode="Markdown")
	except TelegramError as e:
		if str(e) == 'Bad Request: message is not modified (400)':
			pass
		else:
			print e

dispatcher.addHandler(CommandHandler('test', test_command))
"""

dispatcher.addHandler(MessageHandler([Filters.contact], adduser_command))

dispatcher.addHandler(CommandHandler('start', start_command))
dispatcher.addHandler(CommandHandler('help', help_command))
dispatcher.addHandler(CommandHandler('teach', teach_command))

dispatcher.addHandler(CallbackQueryHandler(callback_dispatcher))

dispatcher.addErrorHandler(error)

updater.start_polling()
updater.idle()
