# -*- coding: utf-8 -*-
import os
import json
import logging
import urllib.request
import boto3
import pandas as pd
import re
import requests
from datetime import datetime, date, timedelta
from io import StringIO

# dynamodb用
from boto3.dynamodb.conditions import Key, Attr
import decimal
from botocore.exceptions import ClientError

# S3のクレデンシャル情報は環境設定で指定。
S3_accesskey = os.environ['s3accesskey']
S3_secretkey = os.environ['s3secretkey']
# S3のバケットを指定
Bucket_profile = 'buc-steamapi'

# TrelloのAPIキーとトークンは環境設定で指定。
key = os.environ['Trello_key']
token = os.environ['Trello_token']

# ログ設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def handle_slack_event(slack_event: dict, context) -> str:
    # 受け取ったイベント情報をCloud Watchログに出力
    logging.info(json.dumps(slack_event))

    # Event APIの認証
    if "challenge" in slack_event:
        return slack_event.get("challenge")

    # ボットによるイベントまたはメッセージ投稿イベント以外の場合
    # 反応させないためにそのままリターンする
    # Slackには何かしらのレスポンスを返す必要があるのでOKと返す
    # （返さない場合、失敗とみなされて同じリクエストが何度か送られてくる）
    slack_event_detail: dict = slack_event.get("event")
    # slack_event.get("event").get("subtype")
    # bot発言を拾わないようにするなら削除する。
    # if is_bot(slack_event) :

    #   list_cards = json.loads(json.dumps(slack_event))
    #  post_message_to_slack_channel(list_cards, slack_event.get("event").get("channel"))
    # print(list_cards)
    ##return "OK"

    if is_delete(slack_event):
        post_message_to_slack_channel("コメントを消しましたか？　なぜ消したし、ですよ？", slack_event.get("event").get("channel"))

    if is_message_kitaku(slack_event):
        # Slackにメッセージを投稿する
        post_message_to_slack_channel("お帰りなさい、提督！", slack_event.get("event").get("channel"))

    if is_message_turai(slack_event):
        # Slackにメッセージを投稿する
        post_message_to_slack_channel("つ元気　ですよ、提督。", slack_event.get("event").get("channel"))

    if is_message_oyasumi(slack_event):
        if check_method_can_go_or_not(get_event_time_stamp(slack_event)):
            post_message_to_slack_channel("おやすみなさい提督！　今日もお疲れ様でした。", slack_event.get("event").get("channel"))

    if is_message_ohayo(slack_event):
        # Slackにメッセージを投稿する
        post_message_to_slack_channel("おはようございます、提督！　今日も一日頑張るぞい、ですよ。", slack_event.get("event").get("channel"))

    if is_message_inreview(slack_event):
        # Slackにメッセージを投稿する
        post_message_to_slack_channel("確認しますね。", slack_event.get("event").get("channel"))

        url = "https://api.trello.com/1/lists/{}/cards/?key={}&token={}&fields=name".format('5d19f9f79432a9516d05f647',
                                                                                            key, token)
        r = requests.get(url)
        # 結果はJSON形式なのでデコードする
        list_cards = json.loads(r.text)

        for index, card in enumerate(list_cards):
            # idを指定しているのは、Doneのカラム。
            url = "https://api.trello.com/1/cards/{}/?key={}&token={}&idList={}&fields=name".format(card["id"], key,
                                                                                                    token,
                                                                                                    '5d19f9f8160a3c74114923d5')
            text = "「{}」のタスクですが、内容確認しました！　いいかんじです！　Doneに移しときますね( ´ ▽ ` )".format(card["name"])
            post_message_to_slack_channel(text, slack_event.get("event").get("channel"))
            # putすれば勝手に動かしてくれる。
            r = requests.put(url)

    if is_message_day(slack_event):
        # Slackにメッセージを投稿する
        # print(slack_event_detail.get("text"))
        df = get_csv_from_s3_as_pd_dataframe('processed_data/df_daily_{}.csv'.format(slack_event_detail.get("text")))
        text = str(len(df))
        post_message_to_slack_channel('プレイ時間ですか？　その日は' + text + 'つのタイトルをプレイしていたようですね。',
                                      slack_event.get("event").get("channel"))
        if len(df) != 0:
            for index, row in df.iterrows():
                say = "{}というタイトルについては、どうやら{}分プレイしていたようですね。".format(row['name'], row['playtime_daily'])
                post_message_to_slack_channel(str(say), slack_event.get("event").get("channel"))

    return "OK"


def is_bot(slack_event: dict) -> bool:
    return slack_event.get("event").get("subtype") == "bot_message"


def is_delete(slack_event: dict) -> bool:
    return slack_event.get("event").get("subtype") == "message_deleted"


def is_message_event(slack_event: dict) -> bool:
    return slack_event.get("event").get("type") == "message"


def is_message_kitaku(slack_event: dict) -> bool:
    return slack_event.get("event").get("text") == "帰宅"


def is_message_turai(slack_event: dict) -> bool:
    return slack_event.get("event").get("text") == "つらい"


def is_message_oyasumi(slack_event: dict) -> bool:
    return slack_event.get("event").get("text") == "おやすみ"


def is_message_ts(slack_event: dict) -> bool:
    return slack_event.get("event").get("text") == "ts"


def is_message_ohayo(slack_event: dict) -> bool:
    return slack_event.get("event").get("text") == "おはよう"


def is_message_inreview(slack_event: dict) -> bool:
    r = re.match(r'.*to list "InReview".*', slack_event.get("event").get("attachments")[0]["fallback"])
    # ts = slack_event.get("event").get("ts")
    if r:
        return True
    else:
        return False


# 当該スラックイベントの発生時を取得する。これを持ってスラックイベントのIDとして解釈する。
def get_event_time_stamp(slack_event):
    return slack_event.get("event").get("ts")


def is_message_day(slack_event: dict) -> bool:
    m = re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', slack_event.get("event").get("text"))
    if m:
        return True
        ##post_message_to_slack_channel("日付ですか？　提督。", slack_event.get("event").get("channel"))
    else:
        return False


def get_csv_from_s3_as_pd_dataframe(s3_file_key):
    s3 = boto3.client('s3',
                      aws_access_key_id=S3_accesskey,
                      aws_secret_access_key=S3_secretkey,
                      region_name='ap-northeast-1')

    obj = s3.get_object(Bucket=Bucket_profile, Key=s3_file_key)

    body = obj['Body']

    csv_string = body.read().decode('utf-8')

    return pd.read_csv(StringIO(csv_string))


# 関数を走らせてよいかどうかのチェック
def check_method_can_go_or_not(id):
    # DynamoDBテーブルのオブジェクトを取得
    table_name = "pmbot_controller"
    dynamotable = dynamodb.Table(table_name)

    # クエリを飛ばして、当該IDがすでにテーブル上に登録されているかどうかをチェックする。
    response = dynamotable.query(
        KeyConditionExpression=Key('eventid').eq(id)
    )

    # hitしなかった場合
    if response['ScannedCount'] == 0:
        # dynamodbに書き込んで後続の処理を継続（TRUEを返す）
        write_res = dynamotable.put_item(Item={'eventid': id})
        return True

    # hitした場合
    else:
        # すでに処理が走っているとみなし、何もせずに処理をスルー（Falseを返す）。
        return False


def post_message_to_slack_channel(message: str, channel: str):
    # Slackのchat.postMessage APIを利用して投稿する
    # ヘッダーにはコンテンツタイプとボット認証トークンを付与する
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Authorization": "Bearer {0}".format(os.environ["SLACK_BOT_USER_ACCESS_TOKEN"])
    }
    data = {
        "token": os.environ["SLACK_APP_AUTH_TOKEN"],
        "channel": channel,
        "text": message,
        "username": "PMと化した翔鶴姉bot"
    }
    req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"), method="POST", headers=headers)
    urllib.request.urlopen(req)
    return