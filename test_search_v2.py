import httpx

def test_search_v2():
    print("Testing search-v2 endpoint...")
    # This is the URL the subagent found
    url = "https://gamma-api.polymarket.com/search-v2?q=btc+5m&page=1&limit_per_type=20&type=events&events_status=active&optimized=false"

    try:
        response = httpx.get(
            url,
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=20,
            follow_redirects=False,
        )
        response.raise_for_status()
        data = response.json()
        print(f"Keys in response: {data.keys()}")

        # search-v2 usually returns data categorized by type
        events = data.get('data', [])
        print(f"Found {len(events)} events.")

        for event in events:
            print(f"EVENT: {event.get('title')} | Slug: {event.get('slug')}")
            # Check for markets inside the event
            markets = event.get('markets', [])
            for m in markets:
                print(f"  MARKET: {m.get('question')} | Slug: {m.get('slug')} | Active: {m.get('active')}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_search_v2()
