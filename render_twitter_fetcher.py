#!/usr/bin/env python3
"""
==============================================================================
DUAL-MODE TWITTER FETCHER - USER TIMELINE + KEYWORD SEARCH
==============================================================================
Purpose: Fetch tweets using TWO methods:
         1. BY USERNAME - Get tweets FROM @MariaSTsehai (Maria Sarungi Tsehai)
         2. BY KEYWORD - Get tweets MENTIONING "Samia Suluhu Hassan"

Twitter Account: @MaishaP81252
App Name: taifa
Last Updated: January 6, 2026

Features:
- Fetches tweets from specific Twitter accounts (user timelines)
- Fetches tweets mentioning specific keywords (search)
- Gets FULL text (no truncation on retweets)
- Stores reply_count, quote_count, conversation_id
- Handles datetime JSON serialization properly

==============================================================================
"""

# ==============================================================================
# IMPORT REQUIRED LIBRARIES
# ==============================================================================

import psycopg2
import json
import tweepy
from datetime import datetime
import os

# ==============================================================================
# CONFIGURATION - NEW TWITTER API CREDENTIALS
# ==============================================================================

# DATABASE CONNECTION STRING
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://twitter_data_pygj_user:bBZRdS06QyhBHhBGWRtl4pkLjlaFO5MS@dpg-d5adn26r433s738bha50-a.singapore-postgres.render.com/twitter_data_pygj'
)

# NEW TWITTER API CREDENTIALS (from @MaishaP81252 / taifa app)
CONSUMER_KEY = os.getenv('TWITTER_CONSUMER_KEY', 's49mJ9FlavJeRylWelaXBi2Og')
CONSUMER_SECRET = os.getenv('TWITTER_CONSUMER_SECRET', 'FK08MsWVKnmdeAhniyU4eUdFu5ITh0XQGFXUuglrNtGfUt9yG1')
BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN', 'AAAAAAAAAAAAAAAAAAAAAFA+6wEAAAAA2+w59D8nZsUF28G/J/lbljdSvU8=JzqyxM2pMUdPPeUCfZlhE27uhbhwnHhyHpTUTMGSOEjSy3nWzk')

# ==============================================================================
# CONFIGURATION - WHAT TO FETCH
# ==============================================================================

# METHOD 1: USERNAMES TO TRACK
# Twitter handles (without @) to fetch tweets FROM
USERNAMES_TO_TRACK = [
    "MariaSTsehai",      # Maria Sarungi Tsehai - Journalist/Activist
    # Add more users here:
    # "MaishaP81252",    # Your own account
    # "SuluhuSamia",     # President Samia
    # "tundu_lissu",     # Opposition leader
]

# METHOD 2: KEYWORDS TO SEARCH
# Phrases to search for in ALL tweets
KEYWORDS_TO_SEARCH = [
    "Samia Suluhu Hassan",   # Tweets ABOUT President Samia
    # Add more keywords:
    # "Tundu Lissu",
    # "Tanzania politics",
    # "CCM",
]

# How many tweets to fetch per user/keyword
TWEETS_PER_SOURCE = 5

# ==============================================================================
# FUNCTION 1: FETCH TWEETS FROM SPECIFIC USERS (USER TIMELINE)
# ==============================================================================

def fetch_user_timeline_tweets(client, username, max_results=5):
    """
    Fetch tweets posted BY a specific user
    """
    
    print("Fetching tweets FROM user: @%s" % username)
    
    try:
        # Get User ID from Username
        user_response = client.get_user(
            username=username,
            user_fields=['id', 'name', 'username', 'verified', 'public_metrics']
        )
        
        if not user_response.data:
            print("  User @%s not found" % username)
            return []
        
        user = user_response.data
        user_id = user.id
        
        print("  Found user: %s (@%s)" % (user.name, user.username))
        print("  User ID: %s" % user_id)
        
        # Get Tweets from This User
        tweets_response = client.get_users_tweets(
            id=user_id,
            max_results=max_results,
            tweet_fields=[
                'created_at',
                'public_metrics',
                'lang',
                'conversation_id',
                'referenced_tweets',
                'in_reply_to_user_id'
            ],
            expansions=['referenced_tweets.id'],
        )
        
        if not tweets_response.data:
            print("  No tweets found from @%s" % username)
            return []
        
        # Process Each Tweet
        all_tweets = []
        
        # Build referenced tweets lookup
        referenced = {}
        if tweets_response.includes and 'tweets' in tweets_response.includes:
            referenced = {t.id: t for t in tweets_response.includes['tweets']}
        
        for tweet in tweets_response.data:
            # Get full text
            full_text = tweet.text
            is_retweet = False
            original_tweet_id = None
            
            if tweet.referenced_tweets:
                for ref in tweet.referenced_tweets:
                    if ref.type == 'retweeted':
                        is_retweet = True
                        original_tweet_id = ref.id
                        if ref.id in referenced:
                            original = referenced[ref.id]
                            full_text = "RT @%s: %s" % (username, original.text)
            
            # Extract metrics
            reply_count = tweet.public_metrics.get('reply_count', 0)
            quote_count = tweet.public_metrics.get('quote_count', 0)
            
            tweet_data = {
                'id': tweet.id,
                'text': full_text,
                'created_at': tweet.created_at,
                'author_username': username,
                'retweet_count': tweet.public_metrics['retweet_count'],
                'like_count': tweet.public_metrics['like_count'],
                'reply_count': reply_count,
                'quote_count': quote_count,
                'conversation_id': tweet.conversation_id,
                'is_retweet': is_retweet,
                'original_tweet_id': original_tweet_id,
                'raw_data': {
                    'fetch_method': 'user_timeline',
                    'source_username': username,
                    'language': tweet.lang,
                    'is_mock': False,
                    'source': 'twitter_api_user_timeline',
                    'twitter_account': '@MaishaP81252'
                }
            }
            
            all_tweets.append(tweet_data)
        
        print("  Found %d tweets from @%s" % (len(all_tweets), username))
        return all_tweets
        
    except tweepy.errors.NotFound:
        print("  Error: User @%s does not exist" % username)
        return []
    except Exception as e:
        print("  Error fetching from @%s: %s" % (username, e))
        return []

# ==============================================================================
# FUNCTION 2: FETCH TWEETS BY KEYWORD SEARCH
# ==============================================================================

def fetch_keyword_search_tweets(client, keyword, max_results=5):
    """
    Fetch tweets that MENTION a specific keyword
    """
    
    print("Searching tweets MENTIONING: '%s'" % keyword)
    
    try:
        # Search for Keyword
        response = client.search_recent_tweets(
            query=keyword,
            max_results=max_results,
            tweet_fields=[
                'created_at',
                'public_metrics',
                'lang',
                'author_id',
                'conversation_id',
                'referenced_tweets',
                'in_reply_to_user_id'
            ],
            expansions=[
                'author_id',
                'referenced_tweets.id'
            ],
            user_fields=[
                'username',
                'name',
                'verified',
                'public_metrics'
            ]
        )
        
        if not response.data:
            print("  No tweets found for '%s'" % keyword)
            return []
        
        # Build Lookup Tables
        users = {}
        if response.includes and 'users' in response.includes:
            users = {u.id: u for u in response.includes['users']}
        
        referenced = {}
        if response.includes and 'tweets' in response.includes:
            referenced = {t.id: t for t in response.includes['tweets']}
        
        # Process Each Tweet
        all_tweets = []
        
        for tweet in response.data:
            # Get author info
            author = users.get(tweet.author_id, None)
            if author:
                username = author.username
            else:
                username = "user_%s" % tweet.author_id
            
            # Get full text
            full_text = tweet.text
            is_retweet = False
            original_tweet_id = None
            
            if tweet.referenced_tweets:
                for ref in tweet.referenced_tweets:
                    if ref.type == 'retweeted':
                        is_retweet = True
                        original_tweet_id = ref.id
                        if ref.id in referenced:
                            original = referenced[ref.id]
                            full_text = "RT @%s: %s" % (username, original.text)
            
            # Extract metrics
            reply_count = tweet.public_metrics.get('reply_count', 0)
            quote_count = tweet.public_metrics.get('quote_count', 0)
            
            tweet_data = {
                'id': tweet.id,
                'text': full_text,
                'created_at': tweet.created_at,
                'author_username': username,
                'retweet_count': tweet.public_metrics['retweet_count'],
                'like_count': tweet.public_metrics['like_count'],
                'reply_count': reply_count,
                'quote_count': quote_count,
                'conversation_id': tweet.conversation_id,
                'is_retweet': is_retweet,
                'original_tweet_id': original_tweet_id,
                'raw_data': {
                    'fetch_method': 'keyword_search',
                    'search_keyword': keyword,
                    'language': tweet.lang,
                    'is_mock': False,
                    'source': 'twitter_api_keyword_search',
                    'twitter_account': '@MaishaP81252'
                }
            }
            
            all_tweets.append(tweet_data)
        
        print("  Found %d tweets mentioning '%s'" % (len(all_tweets), keyword))
        return all_tweets
        
    except Exception as e:
        print("  Error searching '%s': %s" % (keyword, e))
        return []

# ==============================================================================
# FUNCTION 3: MAIN FETCH FUNCTION
# ==============================================================================

def fetch_tweets():
    """
    Main function - fetches using BOTH methods
    """
    
    print("=" * 70)
    print("  DUAL-MODE TWITTER FETCHER")
    print("  Twitter Account: @MaishaP81252 | App: taifa")
    print("=" * 70)
    print("[%s] Starting fetch..." % datetime.now())
    print("")
    
    try:
        # Authenticate with Twitter API
        client = tweepy.Client(
            bearer_token=BEARER_TOKEN,
            wait_on_rate_limit=True
        )
        
        print("Connected to Twitter API (Account: @MaishaP81252)")
        print("")
        
        # METHOD 1: Fetch from User Timelines
        print("-" * 70)
        print("METHOD 1: Fetching tweets FROM specific users")
        print("-" * 70)
        
        user_timeline_tweets = []
        
        for username in USERNAMES_TO_TRACK:
            tweets = fetch_user_timeline_tweets(
                client, 
                username, 
                max_results=TWEETS_PER_SOURCE
            )
            user_timeline_tweets.extend(tweets)
            print("")
        
        print("Total from user timelines: %d tweets" % len(user_timeline_tweets))
        print("")
        
        # METHOD 2: Fetch by Keyword Search
        print("-" * 70)
        print("METHOD 2: Searching tweets MENTIONING keywords")
        print("-" * 70)
        
        keyword_search_tweets = []
        
        for keyword in KEYWORDS_TO_SEARCH:
            tweets = fetch_keyword_search_tweets(
                client,
                keyword,
                max_results=TWEETS_PER_SOURCE
            )
            keyword_search_tweets.extend(tweets)
            print("")
        
        print("Total from keyword searches: %d tweets" % len(keyword_search_tweets))
        print("")
        
        # Combine and Store
        all_tweets = user_timeline_tweets + keyword_search_tweets
        
        print("=" * 70)
        print("TOTAL COLLECTED: %d tweets" % len(all_tweets))
        print("  - From user timelines: %d" % len(user_timeline_tweets))
        print("  - From keyword searches: %d" % len(keyword_search_tweets))
        print("=" * 70)
        print("")
        
        if all_tweets:
            store_tweets(all_tweets)
        else:
            print("No tweets to store")
        
        print("[%s] Fetch completed!" % datetime.now())
        print("")
        
    except Exception as e:
        print("Fatal error: %s" % e)

# ==============================================================================
# FUNCTION 4: STORE TWEETS IN DATABASE
# ==============================================================================

def store_tweets(tweets):
    """
    Save tweets to PostgreSQL database
    """
    
    try:
        print("Storing tweets in database...")
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        stored_count = 0
        updated_count = 0
        
        for tweet in tweets:
            try:
                # Convert datetime to string for JSON
                tweet_for_json = tweet.copy()
                
                if isinstance(tweet_for_json['created_at'], datetime):
                    tweet_for_json['created_at'] = tweet_for_json['created_at'].isoformat()
                
                # Insert or Update
                cursor.execute("""
                    INSERT INTO tweets (
                        id,
                        text,
                        author_username,
                        created_at,
                        retweet_count,
                        like_count,
                        raw_data
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        text = EXCLUDED.text,
                        retweet_count = EXCLUDED.retweet_count,
                        like_count = EXCLUDED.like_count,
                        raw_data = EXCLUDED.raw_data
                    RETURNING (xmax = 0) AS inserted
                """, (
                    tweet['id'],
                    tweet['text'],
                    tweet['author_username'],
                    tweet['created_at'],
                    tweet['retweet_count'],
                    tweet['like_count'],
                    json.dumps(tweet_for_json)
                ))
                
                result = cursor.fetchone()
                if result and result[0]:
                    stored_count += 1
                else:
                    updated_count += 1
                
            except Exception as e:
                print("  Error storing tweet %s: %s" % (tweet['id'], e))
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database operation complete:")
        print("  - New tweets stored: %d" % stored_count)
        print("  - Existing tweets updated: %d" % updated_count)
        
    except Exception as e:
        print("Database error: %s" % e)

# ==============================================================================
# MAIN PROGRAM ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    try:
        fetch_tweets()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print("\n\nUnexpected error: %s" % e)

# ==============================================================================
# END OF SCRIPT
# ==============================================================================
