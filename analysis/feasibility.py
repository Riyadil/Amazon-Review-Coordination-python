import gzip, json, re, sys, collections, time
path = sys.argv[1]
DAY, WEEK = 86_400_000, 7 * 86_400_000
norm = lambda t: re.sub(r'[^a-z0-9 ]+', ' ', t.lower()).split()
t0 = time.time()
n = short = 0
short_texts = collections.Counter()
grp_day, grp_week = collections.Counter(), collections.Counter()
user_day = collections.Counter()
dup = collections.defaultdict(set)          # (product, week, text) -> distinct users
with gzip.open(path, 'rt') as fh:
    for line in fh:
        r = json.loads(line); n += 1
        p, u, ts = r['parent_asin'], r['user_id'], r['timestamp']
        words = norm(r.get('text') or '')
        txt = ' '.join(words)
        grp_day[(p, ts // DAY)] += 1
        grp_week[(p, ts // WEEK)] += 1
        user_day[(u, ts // DAY)] += 1
        if len(txt) < 30:
            short += 1; short_texts[txt] += 1
        elif len(words) >= 10:
            dup[(p, ts // WEEK, txt)].add(u)
def dist(c, label):
    v = list(c.values())
    print(f'  {label}: {len(v):,} groups | >=5: {sum(x>=5 for x in v):,} | >=20: {sum(x>=20 for x in v):,} '
          f'| >=100: {sum(x>=100 for x in v):,} | max: {max(v):,}')
print(f'reviews scanned: {n:,}  ({time.time()-t0:.0f}s)')
print(f'\nNOISE - reviews under 30 chars: {short:,} ({100*short/n:.1f}%)')
print('  most common:', ', '.join(f'"{t}" x{c:,}' for t, c in short_texts.most_common(6)))
print('\nSKEW - candidate group sizes')
dist(grp_day, 'product-day ')
dist(grp_week, 'product-week')
big = grp_week.most_common(1)[0]; print(f'  largest product-week group: {big[1]:,} reviews')
print('\nSIGNAL - identical 10+ word text, same product, same week, DIFFERENT accounts')
clusters = [len(s) for s in dup.values() if len(s) >= 2]
print(f'  clusters with >=2 accounts: {len(clusters):,} | >=3: {sum(c>=3 for c in clusters):,} '
      f'| >=5: {sum(c>=5 for c in clusters):,} | largest: {max(clusters) if clusters else 0}')
print(f'  reviews involved: {sum(clusters):,}')
ex = sorted(((len(s), k[2]) for k, s in dup.items() if len(s) >= 3), reverse=True)[:3]
for c, t in ex: print(f'    {c} accounts: "{t[:90]}..."')
print('\nACCOUNT BURSTS - one account, one day')
v = list(user_day.values())
print(f'  accounts with >=10 reviews in a single day: {sum(x>=10 for x in v):,} | max in one day: {max(v)}')
