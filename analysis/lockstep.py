import gzip, json, sys, collections, time
path = sys.argv[1]; WEEK = 7 * 86_400_000; t0 = time.time()
uid = {}; grp = collections.defaultdict(set)
with gzip.open(path, 'rt') as fh:
    for line in fh:
        r = json.loads(line)
        grp[(r['parent_asin'], r['timestamp'] // WEEK)].add(uid.setdefault(r['user_id'], len(uid)))
M = 10_000_000; pc = collections.Counter(); skipped = 0
for s in grp.values():
    n = len(s)
    if n < 2: continue
    if n > 30: skipped += 1; continue
    l = sorted(s)
    for i in range(n):
        a = l[i] * M
        for b in l[i+1:]: pc[a + b] += 1
print(f'accounts: {len(uid):,} | product-weeks: {len(grp):,} | skipped groups >30 accounts: {skipped:,}   ({time.time()-t0:.0f}s)')
print('\nLOCKSTEP - account pairs reviewing the SAME products in the SAME weeks')
v = pc.values()
for k in (2, 3, 5, 10, 20): print(f'  pairs sharing >={k:>2} product-weeks: {sum(x >= k for x in v):,}')
for k in (3, 5):
    par = {}
    def find(x):
        while par.setdefault(x, x) != x: par[x] = par[par[x]]; x = par[x]
        return x
    for key, c in pc.items():
        if c >= k: par[find(key // M)] = find(key % M)
    sizes = collections.Counter(find(x) for x in list(par)).values()
    print(f'  groups linked at >={k} shared product-weeks: {sum(s >= 3 for s in sizes):,} groups of 3+ accounts '
          f'| largest: {max(sizes) if sizes else 0} accounts')
print('  top pairs:', ', '.join(str(c) for _, c in pc.most_common(5)), 'shared product-weeks')
