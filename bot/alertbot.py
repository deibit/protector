import sys
import os

from multiprocessing import Process


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
from utils.db import connect, selects

global_id = []

TOKEN = env.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("[BOT] has no token set. It should be find in environment variables")
    sys.exit()


def start(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(
        "ProtecTor Bot ready",
    )
    global global_id
    channeld_id = update.effective_chat.id
    if not channeld_id in global_id:
        global_id.append(channeld_id)


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
    update.message.reply_text("Help!")


def alerts_command(update: Update, context: CallbackContext) -> None:
    try:
        args = context.args

        if args:
            country = args[0].upper()
            c = connect()
            cursor = c.cursor()
            cursor.execute(selects["alerts"], (country,))
            results = cursor.fetchall()
            if not results:
                update.message.reply_text(f"No result for {country}")
            else:
                text = []
                for result in results:
                    text.append("{1} {3} {4} {5} {6}\n".format(*result))
                update.message.reply_text("".join(text))

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
    message = "hola"
    global global_id
    for i in global_id:
        context.bot.send_message(chat_id=i, text=message)


def main() -> None:
    updater = Updater(TOKEN)
    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("alerts", alerts_command, pass_args=True))
    dispatcher.add_handler(CommandHandler("plot", plot_command, pass_args=True))
    dispatcher.add_handler(CommandHandler("subscribe", subscribe, pass_args=True))
    dispatcher.add_handler(CommandHandler("unsubscribe", unsubscribe, pass_args=True))

    q = updater.job_queue
    q.run_repeating(once, 30)

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
