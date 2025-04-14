#!/usr/bin/env python3
import os
import sys
import time
import logging
import datetime
import subprocess
import schedule
from dotenv import load_dotenv

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("beauty_system.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("BeautyDataSystem")

# 環境変数の読み込み
load_dotenv()

# 設定確認
def check_environment():
    """環境設定の確認"""
    required_vars = [
        "TWITTER_API_KEY",
        "TWITTER_API_SECRET", 
        "TWITTER_ACCESS_TOKEN",
        "TWITTER_ACCESS_SECRET",
        "TWITTER_BEARER_TOKEN"
    ]
    
    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logger.error(f"以下の環境変数が設定されていません: {', '.join(missing)}")
        logger.error("'.env'ファイルを作成し、必要なAPI認証情報を設定してください")
        return False
    
    return True

# 依存関係のインストール
def install_dependencies():
    """必要なパッケージのインストール"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        logger.info("依存関係のインストールが完了しました")
        return True
    except Exception as e:
        logger.error(f"依存関係のインストールに失敗しました: {e}")
        return False

# requirements.txtファイルの作成
def create_requirements():
    """requirements.txtファイルの作成"""
    requirements = [
        "feedparser",
        "pandas",
        "beautifulsoup4",
        "requests",
        "tweepy",
        "schedule",
        "matplotlib",
        "seaborn",
        "nltk",
        "python-dotenv"
    ]
    
    with open("requirements.txt", "w") as f:
        for req in requirements:
            f.write(f"{req}\n")
    
    logger.info("requirements.txtファイルを作成しました")

# RSSフィード収集の実行
def run_rss_collector():
    """RSSフィード収集システムの実行"""
    try:
        logger.info("RSSフィード収集システムを実行します")
        from beauty_rss_collector import fetch_rss_feeds, export_to_csv
        new_entries = fetch_rss_feeds()
        export_to_csv()
        logger.info(f"RSSフィード収集完了: {new_entries}件の新規記事")
        return True
    except Exception as e:
        logger.error(f"RSSフィード収集エラー: {e}")
        return False

# APIデータ収集の実行
def run_api_collector():
    """API経由のデータ収集システムの実行"""
    try:
        logger.info("APIデータ収集システムを実行します")
        from beauty_api_collector import collect_twitter_data, analyze_trends
        collect_twitter_data()
        analyze_trends()
        logger.info("APIデータ収集完了")
        return True
    except Exception as e:
        logger.error(f"APIデータ収集エラー: {e}")
        return False

# トレンド監視の実行
def run_trend_monitor():
    """トレンド監視システムの更新実行"""
    try:
        logger.info("トレンド監視システムを実行します")
        from beauty_trend_monitor import BeautyTrendMonitor
        monitor = BeautyTrendMonitor()
        trends = monitor.update_trends()
        logger.info(f"トレンド監視完了: {len(trends)}個のトレンドを検出")
        return True
    except Exception as e:
        logger.error(f"トレンド監視エラー: {e}")
        return False

# 全システム実行
def run_all_systems():
    """全サブシステムの実行"""
    success = True
    
    if not run_rss_collector():
        success = False
    
    if not run_api_collector():
        success = False
    
    if not run_trend_monitor():
        success = False
    
    if success:
        logger.info("全システム実行完了")
    else:
        logger.warning("一部システムの実行に失敗しました")

# メイン処理
def main():
    logger.info("美容データ収集システム起動")
    
    # 環境のセットアップ
    create_requirements()
    
    if not check_environment():
        logger.error("環境変数の設定を確認してください")
        sys.exit(1)
    
    if not install_dependencies():
        logger.error("依存関係のインストールに失敗しました")
        sys.exit(1)
    
    # 初回実行
    run_all_systems()
    
    # スケジュール設定
    # RSSフィード: 4時間ごと
    schedule.every(4).hours.do(run_rss_collector)
    
    # APIデータ: 2時間ごと
    schedule.every(2).hours.do(run_api_collector)
    
    # トレンド分析: 6時間ごと
    schedule.every(6).hours.do(run_trend_monitor)
    
    # 全システム: 毎日0時
    schedule.every().day.at("00:00").do(run_all_systems)
    
    logger.info("スケジュール設定完了。システム実行中...")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("システム停止（ユーザー割り込み）")
    except Exception as e:
        logger.error(f"予期せぬエラー: {e}")

if __name__ == "__main__":
    main()
