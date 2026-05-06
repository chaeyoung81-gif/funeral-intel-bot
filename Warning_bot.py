#!/usr/bin/env python3
import asyncio
import random
import logging
from datetime import datetime
import pytz
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
BOT_TOKEN = "8638530094:AAEpR293d_qfT9Vuhbbc4kWFtZgCZw_nTTg"
CHAT_ID   = 8563302747
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
KST = pytz.timezone("Asia/Seoul")
ALERT_MESSAGES = [
    "야 채영, 오늘도 인도 달릴 거야? 🛵",
    "벌점 35점인 거 아직 기억해? 한 번만 더 걸리면 면허정지야. 오늘도 인도 탈 거야?",
    "퀴즈~ 채영의 현재 벌점은 몇 점? 틀리면 면허 날아감 ㅋ",
    "범칙금 4만원 = 콜 10개야. 5분 아끼려고 콜 10개 버릴 거야?",
    "경찰 아저씨가 '한 번만 더'라고 했잖아. 기억나? 오늘도 그 '한 번 더' 할 거야?",
    "35점 + 5점 = 40점 = 면허정지. 이 공식 외웠어? 오늘 안전하게 다닐 거야?",
    "솔직히 말해봐, 오늘 인도 탈 생각 있어?",
    "배달하다 경찰 또 만나면 뭐라고 할 거야? 그냥 미리 조심하면 안 돼?",
    "두세 달마다 걸리고, 반성하고, 또 망각하고... 이번엔 다를 거야?",
    "면허정지 되는 날 오면 진짜 후회할 텐데. 오늘 하루만이라도 차도만 다녀줘.",
    "채영아~ 오늘 배달 중에 인도 탈 유혹 올 거 알지? 미리 다짐해.",
    "지금 이 순간 차도로만 가기로 마음 먹어. 할 수 있어?",
    "벌점 35점짜리 라이더, 오늘 어떻게 달릴 예정이야?",
    "이래도 인도 탈 거야? 면허 날아가면 배달 못 해. 수입 0원인 거 알지?",
    "야, 오늘 몇 번 인도 달렸어? 아직 한 번도 안 했으면 오늘도 그렇게 끝내자.",
]
