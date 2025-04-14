import tweepy
import requests
import json
import time
import datetime
import sqlite3
import os
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# Twitter API認証情報
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

# DBの設定
def setup_database():
    conn = sqlite3.connect('beauty_trends.db')
    cursor = conn.cursor()
    
    # X/Twitterトレンド用テーブル
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS twitter_trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT,
        tweet_count INTEGER,
        tweets_text TEXT,
        collection_date TEXT
    )
    ''')
    
    # Instagram用テーブル
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS instagram_trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hashtag TEXT,
        post_count INTEGER,
        recent_posts TEXT,
        collection_date TEXT
    )
    ''')
    
    conn.commit()
    return conn

# 美容関連キーワード
beauty_keywords = [
    "スキンケア", "美容液", "化粧水", "乳液", "クレンジング",
    "メイク", "ファンデーション", "リップ", "アイシャドウ", "マスカラ",
    "ヘアケア", "シャンプー", "トリートメント", "ヘアオイル", "ヘアカラー",
    "美容トレンド", "コスメ", "新作コスメ", "韓国コスメ", "プチプラコスメ",
    # 英語キーワード
    "skincare", "serum", "toner", "moisturizer", "cleansing",
    "makeup", "foundation", "lipstick", "eyeshadow", "mascara",
    "haircare", "shampoo", "treatment", "hair oil", "hair color",
    "beauty trend", "cosmetics", "K-beauty", "J-beauty"
]

# X/Twitter APIでのデータ収集
def collect_twitter_data():
    print("X/Twitterからデータ収集開始...")
    
    try:
        # Twitter API v2クライアントの設定
        client = tweepy.Client(
            bearer_token=TWITTER_BEARER_TOKEN,
            consumer_key=TWITTER_API_KEY, 
            consumer_secret=TWITTER_API_SECRET,
            access_token=TWITTER_ACCESS_TOKEN, 
            access_token_secret=TWITTER_ACCESS_SECRET
        )
        
        conn = setup_database()
        cursor = conn.cursor()
        collection_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for keyword in beauty_keywords:
            try:
                # 各キーワードで検索（最新100件）
                tweets = client.search_recent_tweets(
                    query=f"{keyword} -is:retweet", 
                    max_results=100,
                    tweet_fields=["created_at", "public_metrics"]
                )
                
                if not tweets.data:
                    continue
                    
                # ツイートテキストを集める
                tweets_text = []
                tweet_count = 0
                
                for tweet in tweets.data:
                    tweets_text.append(tweet.text)
                    tweet_count += 1
                
                # DBに保存
                cursor.execute('''
                INSERT INTO twitter_trends (keyword, tweet_count, tweets_text, collection_date)
                VALUES (?, ?, ?, ?)
                ''', (keyword, tweet_count, json.dumps(tweets_text, ensure_ascii=False), collection_date))
                
                print(f"キーワード '{keyword}' について {tweet_count} 件のツイートを収集")
                
                # API制限に配慮して待機
                time.sleep(3)
                
            except Exception as e:
                print(f"Twitter API エラー (キーワード: {keyword}): {e}")
        
        conn.commit()
        conn.close()
        print("X/Twitterデータ収集完了")
        
    except Exception as e:
        print(f"Twitter API 認証/接続エラー: {e}")

# Instagram非公式APIでのハッシュタグデータ収集（ダミー実装）
def collect_instagram_data():
    print("Instagram関連データ収集はAPI制限により実装が複雑です")
    print("現在はWeb Scrapingツールやサードパーティサービスの利用が必要です")
    # 実際の実装はInstagramのAPI変更により変動するため省略

# 収集したデータの分析（例）
def analyze_trends():
    conn = sqlite3.connect('beauty_trends.db')
    cursor = conn.cursor()
    
    # 過去24時間で最も言及された美容キーワードトップ10
    cursor.execute('''
    SELECT keyword, SUM(tweet_count) as total_count
    FROM twitter_trends
    WHERE collection_date >= datetime('now', '-1 day')
    GROUP BY keyword
    ORDER BY total_count DESC
    LIMIT 10
    ''')
    
    results = cursor.fetchall()
    print("\n--- 過去24時間の美容トップトレンド (X/Twitter) ---")
    for i, (keyword, count) in enumerate(results, 1):
        print(f"{i}. {keyword}: {count}ツイート")
    
    conn.close()

if __name__ == "__main__":
    print("美容キーワードAPI検索システム 開始")
    collect_twitter_data()
    # collect_instagram_data()  # 現在は実装省略
    analyze_trends()
