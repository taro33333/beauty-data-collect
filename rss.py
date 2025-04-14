import feedparser
import pandas as pd
import time
import datetime
import sqlite3
from bs4 import BeautifulSoup
import requests

# DBの設定
def setup_database():
    conn = sqlite3.connect('beauty_feeds.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS beauty_articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        link TEXT UNIQUE,
        published TEXT,
        summary TEXT,
        source TEXT,
        keywords TEXT,
        added_date TEXT
    )
    ''')
    conn.commit()
    return conn

# 主要な美容関連RSSフィードリスト
beauty_feeds = [
    {"url": "https://www.allure.com/feed/rss", "source": "Allure"},
    {"url": "https://www.byrdie.com/rss", "source": "Byrdie"},
    {"url": "https://intothegloss.com/feed/", "source": "Into The Gloss"},
    {"url": "https://www.glamour.com/feed/beauty/rss", "source": "Glamour Beauty"},
    {"url": "https://www.refinery29.com/beauty.xml", "source": "Refinery29 Beauty"},
    {"url": "https://www.vogue.com/beauty/feed/rss", "source": "Vogue Beauty"},
    # 日本語のRSSフィード
    {"url": "https://www.cosme.net/feed", "source": "@cosme"},
    {"url": "https://maquia.hpplus.jp/feed", "source": "MAQUIA"},
    {"url": "https://www.biteki.com/feed", "source": "美的"},
]

# キーワードリスト（必要に応じて更新）
beauty_keywords = ["skincare", "makeup", "haircare", "beauty", "cosmetics", 
                  "スキンケア", "メイク", "コスメ", "美容", "ヘアケア"]

def extract_keywords(text, keywords):
    """記事内容からキーワードを抽出"""
    found_keywords = []
    if text:
        text_lower = text.lower()
        for keyword in keywords:
            if keyword.lower() in text_lower:
                found_keywords.append(keyword)
    return ", ".join(found_keywords)

def fetch_rss_feeds():
    """RSSフィードを取得してDBに保存"""
    conn = setup_database()
    cursor = conn.cursor()
    
    total_new_entries = 0
    
    for feed_info in beauty_feeds:
        try:
            feed = feedparser.parse(feed_info["url"])
            print(f"処理中: {feed_info['source']} - エントリー数: {len(feed.entries)}")
            
            for entry in feed.entries:
                title = entry.get("title", "")
                link = entry.get("link", "")
                published = entry.get("published", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                
                # 要約を取得（サマリーがない場合は本文から）
                summary = ""
                if hasattr(entry, "summary"):
                    summary = entry.summary
                elif hasattr(entry, "content"):
                    summary = entry.content[0].value
                
                # HTML要素の除去
                if summary:
                    soup = BeautifulSoup(summary, "html.parser")
                    summary = soup.get_text()
                
                # キーワード抽出
                keywords = extract_keywords(title + " " + summary, beauty_keywords)
                
                # DBに挿入（重複チェック）
                try:
                    cursor.execute('''
                    INSERT INTO beauty_articles (title, link, published, summary, source, keywords, added_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (title, link, published, summary, feed_info["source"], keywords, 
                          datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    conn.commit()
                    total_new_entries += 1
                except sqlite3.IntegrityError:
                    # すでに存在する記事はスキップ
                    pass
                    
        except Exception as e:
            print(f"エラー ({feed_info['source']}): {e}")
    
    conn.close()
    return total_new_entries

def export_to_csv():
    """最新の記事をCSVにエクスポート"""
    conn = sqlite3.connect('beauty_feeds.db')
    df = pd.read_sql_query('''
    SELECT * FROM beauty_articles 
    ORDER BY added_date DESC LIMIT 1000
    ''', conn)
    df.to_csv('beauty_articles_latest.csv', index=False)
    conn.close()
    print(f"CSVエクスポート完了: {len(df)}件")

if __name__ == "__main__":
    print("美容関連RSSフィード収集システム 開始")
    new_entries = fetch_rss_feeds()
    print(f"収集完了: {new_entries}件の新規記事を追加")
    export_to_csv()
