import gzip, json, re, sys, collections, itertools, time
path = sys.argv[1]; WEEK = 7 * 86_400_000; t0 = time.time()
tok = lambda t: re.sub(r'[^a-z0-9 ]+', ' ', t.lower()).split()
uid = {}; groups = collections.defaultdict(list)
first = {}; multi = collections.Counter(); long_reviews = 0
with gzip.open(path, 'rt') as fh:
    for line in fh:
        r = json.loads(line); w = tok(r.get('text') or '')
        if len(w) < 10: continue
        long_reviews += 1
        u = uid.setdefault(r['user_id'], len(uid))
        h = hash(' '.join(w)); f = first.get(h)
        if f is None: first[h] = u
        elif f != u: multi[h] += 1
        key = (r['parent_asin'], r['timestamp'] // WEEK)
        if hash(key) % 20 == 0:                      # deterministic 5% sample of product-weeks
            groups[key].append((u, frozenset(' '.join(w[i:i+3]) for i in range(len(w) - 2))))
pairs = j5 = j8 = g_sampled = g_clust3 = 0; examples = []
for key, items in groups.items():
    if not (2 <= len(items) <= 300): continue
    g_sampled += 1; par = {}
    def find(x):
        while par.setdefault(x, x) != x: par[x] = par[par[x]]; x = par[x]
        return x
    for (u1, s1), (u2, s2) in itertools.combinations(items, 2):
        if u1 == u2 or not s1 or not s2: continue
        pairs += 1; j = len(s1 & s2) / len(s1 | s2)
        if j >= 0.5:
            j5 += 1; par[find(u1)] = find(u2)
        if j >= 0.8: j8 += 1
    sizes = collections.Counter(find(x) for x in list(par))
    if sizes and max(sizes.values()) >= 3:
        g_clust3 += 1; examples.append((max(sizes.values()), key[0]))
print(f'long reviews (10+ words): {long_reviews:,}   ({time.time()-t0:.0f}s)')
print(f'\nNEAR-DUPLICATE TEXT (5% sample, 3-word shingles, different accounts, same product-week)')
print(f'  groups compared: {g_sampled:,} | account pairs compared: {pairs:,}')
print(f'  pairs with similarity >=0.5: {j5:,}  | >=0.8: {j8:,}')
print(f'  groups containing a 3+ account similar-text cluster: {g_clust3}  (x20 => ~{g_clust3*20} in full category)')
for s, p in sorted(examples, reverse=True)[:3]: print(f'    {s} accounts on product {p}')
print(f'\nIDENTICAL long text by different accounts, ANY product/time: {len(multi):,} texts, {sum(multi.values()):,} extra accounts')
