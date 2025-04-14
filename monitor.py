import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import schedule
import time
import json
import datetime
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter
import os
import logging

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='beauty_trends.log'
)
logger = logging.getLogger("BeautyTrendMonitor")

# NLTKのダウンロード（初回のみ）
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

# 日本語と英語のストップワード
stop_words_en = set(stopwords.words('english'))
# 日本語のストップワードは別途定義が必要
stop_words_ja = set(["これ", "それ", "あれ", "この", "その", "あの", "ここ", "そこ", "あそこ", "こちら", "どこ", "だれ", "なに", "なん", "何", "私", "貴方", "貴方方", "我々", "私達", "あの人", "あのかた", "彼女", "彼", "です", "あります", "おります", "います", "は", "が", "の", "に", "を", "で", "と", "や", "へ", "から", "より", "も", "どの", "と", "し", "それで", "しかし"])

# トレンド抽出設定
TREND_THRESHOLD = 5  # 言及回数がこの値以上のキーワードをトレンドとみなす

class BeautyTrendMonitor:
    def __init__(self):
        self.setup_dirs()
        self.current_trends = {}
        self.trend_history = []
        
    def setup_dirs(self):
        """必要なディレクトリを作成"""
        os.makedirs("reports", exist_ok=True)
        os.makedirs("visualizations", exist_ok=True)
    
    def extract_trending_terms(self, texts, lang='en'):
        """テキストコレクションからトレンドワードを抽出"""
        all_words = []
        stop_words = stop_words_en if lang == 'en' else stop_words_ja
        
        for text in texts:
            if not text or not isinstance(text, str):
                continue
                
            # トークン化
            words = word_tokenize(text.lower())
            
            # ストップワード、短い単語、数字を除去
            filtered_words = [word for word in words 
                             if word not in stop_words 
                             and len(word) > 2 
                             and not word.isdigit()
                             and word.isalpha()]
            
            all_words.extend(filtered_words)
        
        # 出現頻度をカウント
        word_counts = Counter(all_words)
        
        # しきい値以上の単語を抽出
        trending_terms = {word: count for word, count in word_counts.items() 
                         if count >= TREND_THRESHOLD}
        
        return trending_terms
    
    def analyze_rss_trends(self):
        """RSSフィードからトレンド抽出"""
        try:
            conn = sqlite3.connect('beauty_feeds.db')
            
            # 過去24時間の記事を取得
            df = pd.read_sql_query('''
            SELECT title, summary, source 
            FROM beauty_articles 
            WHERE added_date >= datetime('now', '-1 day')
            ''', conn)
            
            if df.empty:
                logger.info("過去24時間のRSS記事がありません")
                return {}
            
            # 英語と日本語の記事を分離
            # 簡易的に最初の文字のUnicodeコードポイントで判断
            jp_mask = df['title'].apply(lambda x: ord(str(x)[0]) > 1000 if x and isinstance(x, str) else False)
            
            df_en = df[~jp_mask]
            df_jp = df[jp_mask]
            
            # トレンド抽出
            en_titles = df_en['title'].tolist() + df_en['summary'].tolist()
            jp_titles = df_jp['title'].tolist() + df_jp['summary'].tolist()
            
            en_trends = self.extract_trending_terms(en_titles, 'en')
            jp_trends = self.extract_trending_terms(jp_titles, 'ja')
            
            # 結合
            all_trends = {**en_trends, **jp_trends}
            logger.info(f"RSSから{len(all_trends)}個のトレンドキーワードを抽出")
            
            return all_trends
            
        except Exception as e:
            logger.error(f"RSS分析エラー: {e}")
            return {}
    
    def analyze_twitter_trends(self):
        """Twitterデータからトレンド抽出"""
        try:
            conn = sqlite3.connect('beauty_trends.db')
            
            # 過去24時間のTwitterデータを取得
            df = pd.read_sql_query('''
            SELECT keyword, tweets_text 
            FROM twitter_trends 
            WHERE collection_date >= datetime('now', '-1 day')
            ''', conn)
            
            if df.empty:
                logger.info("過去24時間のTwitterデータがありません")
                return {}
            
            # 各キーワードごとのツイートをパース
            all_tweets = []
            for _, row in df.iterrows():
                try:
                    tweets = json.loads(row['tweets_text'])
                    all_tweets.extend(tweets)
                except:
                    continue
            
            # 英語と日本語のツイートを分離（簡易的に）
            jp_tweets = [t for t in all_tweets if t and isinstance(t, str) and ord(t[0]) > 1000]
            en_tweets = [t for t in all_tweets if t and isinstance(t, str) and t not in jp_tweets]
            
            # トレンド抽出
            en_trends = self.extract_trending_terms(en_tweets, 'en')
            jp_trends = self.extract_trending_terms(jp_tweets, 'ja')
            
            # 結合
            all_trends = {**en_trends, **jp_trends}
            logger.info(f"Twitterから{len(all_trends)}個のトレンドキーワードを抽出")
            
            return all_trends
            
        except Exception as e:
            logger.error(f"Twitter分析エラー: {e}")
            return {}
    
    def update_trends(self):
        """全ソースからのトレンドを更新"""
        logger.info("トレンド更新処理開始")
        
        # 各ソースからトレンドを取得
        rss_trends = self.analyze_rss_trends()
        twitter_trends = self.analyze_twitter_trends()
        
        # トレンドをマージ
        all_trends = {}
        
        # RSSトレンドを追加
        for term, count in rss_trends.items():
            all_trends[term] = all_trends.get(term, 0) + count
        
        # Twitterトレンドを追加
        for term, count in twitter_trends.items():
            all_trends[term] = all_trends.get(term, 0) + count
        
        # 重要度でソート
        sorted_trends = dict(sorted(all_trends.items(), key=lambda x: x[1], reverse=True))
        
        # トップ20のトレンドを保存
        top_trends = {k: sorted_trends[k] for k in list(sorted_trends)[:20]} if sorted_trends else {}
        
        # 時刻とともに保存
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.current_trends = {"timestamp": timestamp, "trends": top_trends}
        self.trend_history.append(self.current_trends)
        
        # 履歴は最大100エントリまで保持
        if len(self.trend_history) > 100:
            self.trend_history.pop(0)
        
        # レポート生成
        self.generate_trend_report()
        self.visualize_trends()
        
        logger.info(f"トレンド更新完了: {len(top_trends)}個のトレンドを検出")
        return top_trends
    
    def generate_trend_report(self):
        """トレンドレポートをJSON形式で保存"""
        if not self.current_trends:
            return
            
        timestamp = self.current_trends["timestamp"].replace(":", "-").replace(" ", "_")
        filename = f"reports/beauty_trends_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.current_trends, f, ensure_ascii=False, indent=2)
        
        logger.info(f"トレンドレポート保存: {filename}")
    
    def visualize_trends(self):
        """トレンド可視化（棒グラフ）"""
        if not self.current_trends or not self.current_trends.get("trends"):
            return
        
        trends = self.current_trends["trends"]
        timestamp = self.current_trends["timestamp"].replace(":", "-").replace(" ", "_")
        
        # トップ15のトレンドを可視化
        top_n = dict(list(trends.items())[:15])
        
        plt.figure(figsize=(12, 8))
        sns.barplot(x=list(top_n.values()), y=list(top_n.keys()))
        plt.title(f"美容トレンドワード分析: {self.current_trends['timestamp']}")
        plt.xlabel("言及回数")
        plt.ylabel("トレンドキーワード")
        plt.tight_layout()
        
        # 保存
        filename = f"visualizations/trend_viz_{timestamp}.png"
        plt.savefig(filename)
        plt.close()
        
        logger.info(f"トレンド可視化保存: {filename}")
    
    def run_scheduled_job(self):
        """定期実行ジョブ"""
        self.update_trends()
    
    def start_monitoring(self):
        """モニタリング開始"""
        logger.info("美容トレンド監視システム起動")
        
        # 初回実行
        self.update_trends()
        
        # スケジュール設定（6時間ごと）
        schedule.every(6).hours.do(self.run_scheduled_job)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("システム停止（ユーザー割り込み）")
        except Exception as e:
            logger.error(f"エラー発生: {e}")

if __name__ == "__main__":
    monitor = BeautyTrendMonitor()
    monitor.start_monitoring()
