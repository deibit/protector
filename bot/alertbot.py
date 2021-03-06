import sys
import os

from multiprocessing import Process

import flag


def plot(country):
    import pandas
    import matplotlib.pyplot as plt

    # country = args[0].upper()
    c = connect()
    cursor = c.cursor()
    cursor.execute(selects["users"], (country,))
    results = cursor.fetchall()

    if not results:
        return False
    else:
        filename = f"img_for_{country}.png"
        df = pandas.DataFrame(
            results,
            columns=[
                "id",
                "date",
                "country",
                "users",
                "lower",
                "upper",
                "frac",
            ],
        )
        df.plot(kind="line", x="date", y="users", color="blue")
        plt.savefig(filename)
        return True


from telegram import Update
from telegram.ext import Updater, CallbackContext, CommandHandler
from telegram import (
    Update,
)
from telegram.ext import Updater, CommandHandler, CallbackContext


from utils.env import env
from utils.log import logger
from utils.db import connect, selects, updates


def header():
    return "DATE | COUNTRY | VARIATION | TREND | MAX | MIN | DETECTOR\n"


def translate_alert(alerts):
    """This functions takes a list of alerts tuples and transform them into formatted strings
    Returns a list of formatted strings
    """
    f = lambda x: flag.flag(x[2])
    d = lambda x: x[1].date()
    ud = lambda x: "🔼" if x == "UP" else "🔻"
    n = lambda x: "{0:>6}".format(x)
    formatted = []
    formatted.append(header())
    for alert in alerts:
        formatted.append(
            f"{d(alert)} {f(alert)} {n(alert[3])} {ud(alert[4])} {n(alert[5])} {n(alert[6])} ({alert[7]})\n"
        )
    return formatted


global_id = []

TOKEN = env.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("[BOT] has no token set. It should be find in environment variables")
    sys.exit()


ALERTS_INTERVAL = int(env.get("ALERTS_INTERVAL"))


def subscribe(update: Update, context: CallbackContext) -> None:
    global global_id
    user = update.message.from_user

    channeld_id = update.effective_chat.id
    if not channeld_id in global_id:
        global_id.append(channeld_id)
    update.message.reply_text(
        f"Hi {user.username} you have been subscribe to ProtecTor alerts",
    )


def unsubscribe(update: Update, context: CallbackContext) -> None:
    global global_id
    user = update.message.from_user

    channeld_id = update.effective_chat.id

    if channeld_id in global_id:
        global_id.remove(channeld_id)

    update.message.reply_text(
        f"Hi {user.username} you have been unsubscribe from ProtecTor alerts",
    )


def help_command(update: Update, context: CallbackContext) -> None:
    help_text = """
    Available commands:
        - 'subscribe': Will subscribe you to alerts
        - 'unsubscribe': Stop receiving alerts
        - 'plot <country_code>': Draw a plot on per country users statistics
        - 'alerts': Publish a list of the latest 20 alerts
    """
    update.message.reply_text(help_text)


def alerts_command(update: Update, context: CallbackContext) -> None:
    try:
        c = connect()
        cursor = c.cursor()
        cursor.execute(selects["alerts"])
        results = cursor.fetchall()
        if not results:
            update.message.reply_text(f"No results yet")
        else:
            results = translate_alert(results)
            update.message.reply_text("".join(results))

    except Exception as e:
        logger.exception(e)


def plot_command(update: Update, context: CallbackContext) -> None:
    try:
        args = context.args

        if args:
            country = args[0]
            p = Process(target=plot, args=(country,))
            p.start()
            p.join()
            filename = f"img_for_{country}.png"
            chat_id = update.message.chat_id
            context.bot.send_photo(chat_id, photo=open(filename, "rb"))
            os.remove(filename)
            update.message.reply_text(f"Here you go")
        else:
            update.message.reply_text("Need a country code to work with")

    except Exception as e:
        logger.exception(e)


def once(context: CallbackContext) -> None:
    message = ""
    global global_id
    if not global_id:
        return

    try:
        c = connect()
        cursor = c.cursor()
        cursor.execute(selects["newalerts"])
        results = cursor.fetchall()
        if not results:
            message = "No alerts so far"
        else:
            results = translate_alert(results)
            message = "".join(results)
            cursor.execute(updates["marksentalerts"])
            c.commit()

    except Exception as e:
        logger.exception(e)

    for i in global_id:
        context.bot.send_message(chat_id=i, text=message)


def main() -> None:
    updater = Updater(TOKEN)
    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("alerts", alerts_command))
    dispatcher.add_handler(CommandHandler("plot", plot_command, pass_args=True))
    dispatcher.add_handler(CommandHandler("subscribe", subscribe))
    dispatcher.add_handler(CommandHandler("unsubscribe", unsubscribe))

    q = updater.job_queue
    q.run_repeating(once, ALERTS_INTERVAL)

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
