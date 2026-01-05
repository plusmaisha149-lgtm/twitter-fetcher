#!/usr/bin/env python3
"""
Twitter Fetcher for Render Cloud
Runs in Singapore where Twitter is accessible
"""

import psycopg2
import json
import tweepy
from datetime import datetime
import os

# Database URL (from environment or hardcoded)
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://twitter_data_pygj_user:bBZRdS06QyhBHhBGWRtl4pkLjlaFO5MS@dpg-d5adn26r433s738bha50-a.singapore-postgres.render.com/twitter_data_pygj')

# Twitter API credentials (from environment or hardcoded)
BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN', 'TWITTER_BEARER_TOKEN', 'AAAAAAAAAAAAAAAAAAAAACi4zgEAAAAA0qATv6QklrjGbF8Pe9hdr1mavRk%3DTEwNMv92PRuxVfEbZfHxx098RMIbBzRyOMlNO1lsfXsA1GDr9U')

def fetch_tweets():
    """Fetch tweets from Twitter API"""
    print(f"[{datetime.now()}] Starting Twitter fetch...")

    try:
        # Initialize Twitter client
        client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)
        print("? Connected to Twitter API")

        # Search queries
        queries = [
            "Samia Suluhu Hassan",
            "Tundu Lissu",
            "Tanzania politics",
            "Tanzania"
        ]

        all_tweets = []

        for query in queries:
            print(f"?? Searching: {query}")
            try:
                response = client.search_recent_tweets(
                    query=query,
                    max_results=25,
                    tweet_fields=['created_at', 'public_metrics', 'lang', 'author_id'],
                    expansions=['author_id'],
                    user_fields=['username']
                )

                if response.data:
                    users = {u.id: u for u in response.includes.get('users', [])} if response.includes else {}

                    for tweet in response.data:
                        username = users[tweet.author_id].username if tweet.author_id in users else f"user_{tweet.author_id}"

                        all_tweets.append({
                            'id': tweet.id,
                            'text': tweet.text,
                            'author_username': username,
                            'created_at': tweet.created_at,
                            'retweet_count': tweet.public_metrics['retweet_count'],
                            'like_count': tweet.public_metrics['like_count'],
                            'raw_data': {
                                'source': 'real_twitter_api',
                                'language': tweet.lang,
                                'query': query,
                                'is_mock': False
                            }
                        })
                    print(f"  ? Found {len(response.data)} tweets")
                else:
                    print(f"  ?? No tweets for '{query}'")

            except Exception as e:
                print(f"  ? Error: {e}")
                continue

        print(f"\n?? Total collected: {len(all_tweets)} tweets")

        # Store in database
        if all_tweets:
            store_tweets(all_tweets)

        print(f"[{datetime.now()}] Fetch completed!\n")

    except Exception as e:
        print(f"? Fatal error: {e}")

def store_tweets(tweets):
    """Store tweets in PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        stored = 0
        for tweet in tweets:
            try:
                cursor.execute("""
                    INSERT INTO tweets (id, text, author_username, created_at,
                                       retweet_count, like_count, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    tweet['id'], tweet['text'], tweet['author_username'],
                    tweet['created_at'], tweet['retweet_count'],
                    tweet['like_count'], json.dumps(tweet['raw_data'])
                ))

                if cursor.rowcount > 0:
                    stored += 1
            except:
                continue

        conn.commit()
        cursor.close()
        conn.close()

        print(f"?? Stored {stored} new tweets in database")

    except Exception as e:
        print(f"? Database error: {e}")

if __name__ == "__main__":

    fetch_tweets()
