# -*- coding: utf-8 -*-
import asyncio
import logging
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/home/ubuntu/income_bot/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TOKEN = "8650224345:AAEiS2khWhp3HcEMjvELBo8dpXELPCtiZdU"
CHAT_ID = 8563302747
INSURANCE = 20080
SHEET_ID = "1im5UpkHikSwuqSZo0kuKS09yQHP9EMzLTu9_WZdtIRM"
CREDS_FILE = "/home/ubuntu/income_bot/service_account.json"

def get_sheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet('수입기록')
    except:
        ws = sh.add_worksheet('수입기록', 1000, 10)
        ws.append_row(['날짜', '수입', '시작', '종료', '근무시간', '배달건수', '주유비', '비고'])
    return ws

def save_income(date_str, amount, start_h, end_h, delivery_count, fuel_cost, memo=''):
    try:
        ws = get_sheet()
        hours = end_h - start_h
        ws.append_row([date_str, amount, start_h, end_h, hours, delivery_count, fuel_cost, memo])
        logger.info(f'저장 성공: {date_str} 수입 {amount}원')
    except Exception as e:
        logger.error(f'저장 실패: {e}')
        raise

def get_monthly_summary(date_str):
    ws = get_sheet()
    records = ws.get_all_records()
    ym = date_str[:7]
    total_a = total_h = total_d = total_f = 0
    for r in records:
        if str(r.get('날짜', '')).startswith(ym):
            total_a += int(r.get('수입', 0) or 0)
            total_h += int(r.get('근무시간', 0) or 0)
            total_d += int(r.get('배달건수', 0) or 0)
            total_f += int(r.get('주유비', 0) or 0)
    return total_a, total_h, total_d, total_f

user_state = {}

async def ask_income(context):
    logger.info('15시 수입 알림 발송 시작')
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    await context.bot.send_message(chat_id=CHAT_ID,
        text=f'[{yesterday}] 어제 수입은 얼마?\n\n예: 150000 / 휴무: 0')
    user_state[CHAT_ID] = {'step': 'waiting_amount', 'record_date': yesterday}

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('[수입&지울 알림봇]\n\n매일 15시 어제 수입 입력\n지울일 D-7,D-3,당일 알림\n월말 결산\n\n수동입력: /income\n특정날짜: /income 2026-05-01')

async def income_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    record_date = args[0] if args else (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    await update.message.reply_text(f'[{record_date}] 어제 수입은 얼마?\n\n예: 150000 / 휴무: 0')
    user_state[update.effective_chat.id] = {'step': 'waiting_amount', 'record_date': record_date}

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    text = update.message.text.strip()
    state = user_state.get(cid, {})
    if state.get('step') == 'waiting_amount':
        try:
            clean = text.replace(',','').replace('원','').replace(' ','')
            if '만' in clean:
                parts = clean.split('만')
                amount = int(parts[0]) * 10000 + (int(parts[1]) if parts[1] else 0)
            else:
                amount = int(clean)
            user_state[cid]['amount'] = amount
            user_state[cid]['step'] = 'waiting_start'
            buttons = [
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(6,10)],
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(10,14)],
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(14,18)],
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(18,22)],
                [InlineKeyboardButton('휴무 (모든 입력 건너끄기)', callback_data='start_off')],
            ]
            await update.message.reply_text('근무 시작 시각 선택', reply_markup=InlineKeyboardMarkup(buttons))
        except:
            await update.message.reply_text('숫자로 입력해주세요. 예: 150000')
    elif state.get('step') == 'waiting_delivery':
        try:
            count = int(text.replace('건','').strip())
            user_state[cid]['delivery_count'] = count
            user_state[cid]['step'] = 'waiting_fuel'
            await update.message.reply_text('어제 주유비는 얼마?',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('주유 안함', callback_data='fuel_none')]]))
        except:
            await update.message.reply_text('숫자로 입력해주세요. 예: 50')
    elif state.get('step') == 'waiting_fuel':
        try:
            fuel = int(text.replace(',','').replace('원','').strip())
            user_state[cid]['fuel_cost'] = fuel
            user_state[cid]['step'] = 'waiting_memo'
            await update.message.reply_text('비고를 입력해주세요 (날씨, 메모 등)',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('건너끄기', callback_data='memo_skip')]]))
        except:
            await update.message.reply_text('숫자로 입력해주세요. 예: 15000')
    elif state.get('step') == 'waiting_memo':
        await finish_record(update.message.reply_text, cid, state, text)
    else:
        await update.message.reply_text('수입 입력은 /income 을 눈러주세요!')

async def finish_record(reply_func, cid, state, memo):
    amount = state['amount']
    start_h = state.get('start_hour', 0)
    end_h = state.get('end_hour', 0)
    delivery_count = state.get('delivery_count', 0)
    fuel_cost = state.get('fuel_cost', 0)
    record_date = state['record_date']
    save_income(record_date, amount, start_h, end_h, delivery_count, fuel_cost, memo)
    total_a, total_h, total_d, total_f = get_monthly_summary(record_date)
    hours = end_h - start_h
    user_state.pop(cid, None)
    memo_str = f'\n비고: {memo}' if memo else ''
    await reply_func(
        f'[기록완료]\n날짜: {record_date}\n수입: {amount:,}원\n'
        f'근무: {start_h}시~{end_h}시 ({hours}시간)\n배달: {delivery_count}건{memo_str}\n\n'
        f'[{record_date[:7]} 누적]\n수입: {total_a:,}원 / 근무: {total_h}시간 / 배달: {total_d}건')

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cid = query.message.chat_id
    data = query.data
    state = user_state.get(cid, {})
    if data.startswith('start_') and state.get('step') == 'waiting_start':
        if data == 'start_off':
            user_state[cid]['start_hour'] = 0
            user_state[cid]['end_hour'] = 0
            user_state[cid]['delivery_count'] = 0
            user_state[cid]['fuel_cost'] = 0
            user_state[cid]['step'] = 'waiting_memo'
            await query.edit_message_text('비고를 입력해주세요',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('건너끄기', callback_data='memo_skip')]]))
        else:
            h = int(data.split('_')[1])
            user_state[cid]['start_hour'] = h
            user_state[cid]['step'] = 'waiting_end'
            buttons = []
            row = []
            for i in range(h+1, 25):
                row.append(InlineKeyboardButton(f'{i}시', callback_data=f'end_{i}'))
                if len(row) == 4:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            await query.edit_message_text('근무 종료 시각 선택', reply_markup=InlineKeyboardMarkup(buttons))
    elif data.startswith('end_') and state.get('step') == 'waiting_end':
        end_h = int(data.split('_')[1])
        user_state[cid]['end_hour'] = end_h
        user_state[cid]['step'] = 'waiting_delivery'
        await query.edit_message_text('어제 배달 건수는 몇 건? 숫자만 입력\n예: 50')
    elif data == 'fuel_none' and state.get('step') == 'waiting_fuel':
        user_state[cid]['fuel_cost'] = 0
        user_state[cid]['step'] = 'waiting_memo'
        await query.edit_message_text('비고를 입력해주세요',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('건너끄기', callback_data='memo_skip')]]))
    elif data == 'memo_skip' and state.get('step') == 'waiting_memo':
        await finish_record(query.edit_message_text, cid, state, '')

async def monthly_summary(context):
    logger.info('월간 결산 시작')
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    total_a, total_h, total_d, total_f = get_monthly_summary(yesterday)
    avg = total_a // total_h if total_h else 0
    real_income = total_a - total_f - INSURANCE
    await context.bot.send_message(chat_id=CHAT_ID,
        text=f'[{yesterday[:7]} 최종 결산]\n\n수입: {total_a:,}원\n근무: {total_h}시간\n배달: {total_d}건\n평균시급: {avg:,}원\n\n[지울]\n주유비: {total_f:,}원\n보험료: {INSURANCE:,}원\n\n실수익: {real_income:,}원')

EXPENSES = [
    (1, '노란우산공제', 50000),
    (4, '방세', 350000),
    (10, '태권도학원비', 240000),
    (14, '롯데카드+신한후불교통카드', 0),
    (17, '케이뷱크대출', 233000),
    (23, '한국금융주택', 147000),
    (26, '링키영어', 460000),
]

async def check_expense_alerts(context):
    logger.info('지출 알림 체크 시작')
    today = date.today()
    for day, label, amount in EXPENSES:
        try:
            target = today.replace(day=day)
        except:
            continue
        diff = (target - today).days
        if diff in [7, 3, 0]:
            timing = '오늘 결제일!' if diff==0 else f'D-{diff} ({target.month}월{target.day}일)'
            amt_str = f' {amount:,}원' if amount else ''
            await context.bot.send_message(chat_id=CHAT_ID, text=f'[지울알림] {label}{amt_str}\n{timing}')

async def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler('income', income_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    scheduler = AsyncIOScheduler(timezone='Asia/Seoul')
    scheduler.add_job(ask_income, 'cron', day_of_week='tue-sun', hour=15, minute=0, args=[app])
    scheduler.add_job(check_expense_alerts, 'cron', hour=9, minute=0, args=[app])
    scheduler.add_job(monthly_summary, 'cron', day='1', hour=22, minute=0, args=[app])
    scheduler.start()
    logger.info('=== 봇 시작 ===')
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
ubuntu@funeral-intel-server-2:~$ python3 << 'EOF'
> content = open('/home/ubuntu/income_bot/bot.py', 'r', encoding='utf-8').read()
> content = content.replace('수입&지울 알림봇', '수입&지출 알림봇')
> content = content.replace('지울일 D-7', '지출일 D-7')
> content = content.replace('건너끄기', '건너뛰기')
> content = content.replace('눈러주세요', '눌러주세요')
> content = content.replace('[지울]', '[지출]')
> content = content.replace('케이뷱크대출', 'K뱅크대출')
> open('/home/ubuntu/income_bot/bot.py', 'w', encoding='utf-8').write(content)
> print('done')
> EOF
done
ubuntu@funeral-intel-server-2:~$ sudo systemctl restart income-bot
ubuntu@funeral-intel-server-2:~$ sleep 3 && sudo systemctl status income-bot
● income-bot.service - Income Bot
     Loaded: loaded (/etc/systemd/system/income-bot.service; enabled; vendor preset: enabled)
     Active: active (running) since Wed 2026-05-06 09:04:12 UTC; 8s ago
   Main PID: 65443 (python3)
      Tasks: 2 (limit: 1053)
     Memory: 44.3M
        CPU: 1.759s
     CGroup: /system.slice/income-bot.service
             └─65443 /usr/bin/python3 -u /home/ubuntu/income_bot/bot.py

May 06 09:04:12 funeral-intel-server-2 systemd[1]: Started Income Bot.
ubuntu@funeral-intel-server-2:~$ cat ~/income_bot/bot.py
# -*- coding: utf-8 -*-
import asyncio
import logging
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/home/ubuntu/income_bot/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TOKEN = "8650224345:AAEiS2khWhp3HcEMjvELBo8dpXELPCtiZdU"
CHAT_ID = 8563302747
INSURANCE = 20080
SHEET_ID = "1im5UpkHikSwuqSZo0kuKS09yQHP9EMzLTu9_WZdtIRM"
CREDS_FILE = "/home/ubuntu/income_bot/service_account.json"

def get_sheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet('수입기록')
    except:
        ws = sh.add_worksheet('수입기록', 1000, 10)
        ws.append_row(['날짜', '수입', '시작', '종료', '근무시간', '배달건수', '주유비', '비고'])
    return ws

def save_income(date_str, amount, start_h, end_h, delivery_count, fuel_cost, memo=''):
    try:
        ws = get_sheet()
        hours = end_h - start_h
        ws.append_row([date_str, amount, start_h, end_h, hours, delivery_count, fuel_cost, memo])
        logger.info(f'저장 성공: {date_str} 수입 {amount}원')
    except Exception as e:
        logger.error(f'저장 실패: {e}')
        raise

def get_monthly_summary(date_str):
    ws = get_sheet()
    records = ws.get_all_records()
    ym = date_str[:7]
    total_a = total_h = total_d = total_f = 0
    for r in records:
        if str(r.get('날짜', '')).startswith(ym):
            total_a += int(r.get('수입', 0) or 0)
            total_h += int(r.get('근무시간', 0) or 0)
            total_d += int(r.get('배달건수', 0) or 0)
            total_f += int(r.get('주유비', 0) or 0)
    return total_a, total_h, total_d, total_f

user_state = {}

async def ask_income(context):
    logger.info('15시 수입 알림 발송 시작')
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    await context.bot.send_message(chat_id=CHAT_ID,
        text=f'[{yesterday}] 어제 수입은 얼마?\n\n예: 150000 / 휴무: 0')
    user_state[CHAT_ID] = {'step': 'waiting_amount', 'record_date': yesterday}

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('[수입&지출 알림봇]\n\n매일 15시 어제 수입 입력\n지출일 D-7,D-3,당일 알림\n월말 결산\n\n수동입력: /income\n특정날짜: /income 2026-05-01')

async def income_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    record_date = args[0] if args else (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    await update.message.reply_text(f'[{record_date}] 어제 수입은 얼마?\n\n예: 150000 / 휴무: 0')
    user_state[update.effective_chat.id] = {'step': 'waiting_amount', 'record_date': record_date}

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    text = update.message.text.strip()
    state = user_state.get(cid, {})
    if state.get('step') == 'waiting_amount':
        try:
            clean = text.replace(',','').replace('원','').replace(' ','')
            if '만' in clean:
                parts = clean.split('만')
                amount = int(parts[0]) * 10000 + (int(parts[1]) if parts[1] else 0)
            else:
                amount = int(clean)
            user_state[cid]['amount'] = amount
            user_state[cid]['step'] = 'waiting_start'
            buttons = [
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(6,10)],
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(10,14)],
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(14,18)],
                [InlineKeyboardButton(f'{h}시', callback_data=f'start_{h}') for h in range(18,22)],
                [InlineKeyboardButton('휴무 (모든 입력 건너뛰기)', callback_data='start_off')],
            ]
            await update.message.reply_text('근무 시작 시각 선택', reply_markup=InlineKeyboardMarkup(buttons))
        except:
            await update.message.reply_text('숫자로 입력해주세요. 예: 150000')
    elif state.get('step') == 'waiting_delivery':
        try:
            count = int(text.replace('건','').strip())
            user_state[cid]['delivery_count'] = count
            user_state[cid]['step'] = 'waiting_fuel'
            await update.message.reply_text('어제 주유비는 얼마?',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('주유 안함', callback_data='fuel_none')]]))
        except:
            await update.message.reply_text('숫자로 입력해주세요. 예: 50')
    elif state.get('step') == 'waiting_fuel':
        try:
            fuel = int(text.replace(',','').replace('원','').strip())
            user_state[cid]['fuel_cost'] = fuel
            user_state[cid]['step'] = 'waiting_memo'
            await update.message.reply_text('비고를 입력해주세요 (날씨, 메모 등)',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('건너뛰기', callback_data='memo_skip')]]))
        except:
            await update.message.reply_text('숫자로 입력해주세요. 예: 15000')
    elif state.get('step') == 'waiting_memo':
        await finish_record(update.message.reply_text, cid, state, text)
    else:
        await update.message.reply_text('수입 입력은 /income 을 눌러주세요!')

async def finish_record(reply_func, cid, state, memo):
    amount = state['amount']
    start_h = state.get('start_hour', 0)
    end_h = state.get('end_hour', 0)
    delivery_count = state.get('delivery_count', 0)
    fuel_cost = state.get('fuel_cost', 0)
    record_date = state['record_date']
    save_income(record_date, amount, start_h, end_h, delivery_count, fuel_cost, memo)
    total_a, total_h, total_d, total_f = get_monthly_summary(record_date)
    hours = end_h - start_h
    user_state.pop(cid, None)
    memo_str = f'\n비고: {memo}' if memo else ''
    await reply_func(
        f'[기록완료]\n날짜: {record_date}\n수입: {amount:,}원\n'
        f'근무: {start_h}시~{end_h}시 ({hours}시간)\n배달: {delivery_count}건{memo_str}\n\n'
        f'[{record_date[:7]} 누적]\n수입: {total_a:,}원 / 근무: {total_h}시간 / 배달: {total_d}건')

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cid = query.message.chat_id
    data = query.data
    state = user_state.get(cid, {})
    if data.startswith('start_') and state.get('step') == 'waiting_start':
        if data == 'start_off':
            user_state[cid]['start_hour'] = 0
            user_state[cid]['end_hour'] = 0
            user_state[cid]['delivery_count'] = 0
            user_state[cid]['fuel_cost'] = 0
            user_state[cid]['step'] = 'waiting_memo'
            await query.edit_message_text('비고를 입력해주세요',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('건너뛰기', callback_data='memo_skip')]]))
        else:
            h = int(data.split('_')[1])
            user_state[cid]['start_hour'] = h
            user_state[cid]['step'] = 'waiting_end'
            buttons = []
            row = []
            for i in range(h+1, 25):
                row.append(InlineKeyboardButton(f'{i}시', callback_data=f'end_{i}'))
                if len(row) == 4:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            await query.edit_message_text('근무 종료 시각 선택', reply_markup=InlineKeyboardMarkup(buttons))
    elif data.startswith('end_') and state.get('step') == 'waiting_end':
        end_h = int(data.split('_')[1])
        user_state[cid]['end_hour'] = end_h
        user_state[cid]['step'] = 'waiting_delivery'
        await query.edit_message_text('어제 배달 건수는 몇 건? 숫자만 입력\n예: 50')
    elif data == 'fuel_none' and state.get('step') == 'waiting_fuel':
        user_state[cid]['fuel_cost'] = 0
        user_state[cid]['step'] = 'waiting_memo'
        await query.edit_message_text('비고를 입력해주세요',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('건너뛰기', callback_data='memo_skip')]]))
    elif data == 'memo_skip' and state.get('step') == 'waiting_memo':
        await finish_record(query.edit_message_text, cid, state, '')

async def monthly_summary(context):
    logger.info('월간 결산 시작')
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    total_a, total_h, total_d, total_f = get_monthly_summary(yesterday)
    avg = total_a // total_h if total_h else 0
    real_income = total_a - total_f - INSURANCE
    await context.bot.send_message(chat_id=CHAT_ID,
        text=f'[{yesterday[:7]} 최종 결산]\n\n수입: {total_a:,}원\n근무: {total_h}시간\n배달: {total_d}건\n평균시급: {avg:,}원\n\n[지출]\n주유비: {total_f:,}원\n보험료: {INSURANCE:,}원\n\n실수익: {real_income:,}원')

EXPENSES = [
    (1, '노란우산공제', 50000),
    (4, '방세', 350000),
    (10, '태권도학원비', 240000),
    (14, '롯데카드+신한후불교통카드', 0),
    (17, 'K뱅크대출', 233000),
    (23, '한국금융주택', 147000),
    (26, '링키영어', 460000),
]

async def check_expense_alerts(context):
    logger.info('지출 알림 체크 시작')
    today = date.today()
    for day, label, amount in EXPENSES:
        try:
            target = today.replace(day=day)
        except:
            continue
        diff = (target - today).days
        if diff in [7, 3, 0]:
            timing = '오늘 결제일!' if diff==0 else f'D-{diff} ({target.month}월{target.day}일)'
            amt_str = f' {amount:,}원' if amount else ''
            await context.bot.send_message(chat_id=CHAT_ID, text=f'[지울알림] {label}{amt_str}\n{timing}')

async def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler('income', income_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    scheduler = AsyncIOScheduler(timezone='Asia/Seoul')
    scheduler.add_job(ask_income, 'cron', day_of_week='tue-sun', hour=15, minute=0, args=[app])
    scheduler.add_job(check_expense_alerts, 'cron', hour=9, minute=0, args=[app])
    scheduler.add_job(monthly_summary, 'cron', day='1', hour=22, minute=0, args=[app])
    scheduler.start()
    logger.info('=== 봇 시작 ===')
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
