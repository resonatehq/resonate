# Run TLC on a Check module repeatedly; each violated guarantee is recorded
# and dropped from the cfg, until the run is clean or the cap says the rest
# survived to whatever depth the cap allowed.
import re, subprocess, sys, time

mod, cap = sys.argv[1], int(sys.argv[2])
jar = '/tmp/claude-0/-home-user/6d699f02-a2f6-53ea-8a6a-b3d76a365187/scratchpad/tla2tools.jar'
md  = f'/tmp/claude-0/-home-user/6d699f02-a2f6-53ea-8a6a-b3d76a365187/scratchpad/sw_{mod}'
failed = []

while True:
    try:
        r = subprocess.run(
            ['java','-XX:+UseParallelGC','-cp',jar,'tlc2.TLC','-config',mod+'.cfg',
             '-workers','3','-metadir',md,'-cleanup',mod],
            capture_output=True, text=True, timeout=cap)
        out = r.stdout
        capped = False
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b'').decode() if isinstance(e.stdout, bytes) else (e.stdout or '')
        capped = True

    name = None
    m = re.search(r'Action property (prop_\w+) is violated', out)
    if m:
        name = m.group(1)
    if not name:
        m = re.search(r'Invariant (inv_\w+) is violated', out)
    if m:
        name = m.group(1)
    else:
        m = re.search(r'Action property line (\d+),.*of module (\w+) is violated', out)
        if m:
            line, where = int(m.group(1)), m.group(2)
            if where == mod:
                src = open(mod + '.tla').read().split('\n')
                pat = r'^(prop_\w+) =='
            else:
                src = open(where + '.tla').read().split('\n')
                pat = r'^([a-z][A-Za-z0-9_]*) =='
            for i in range(line - 1, -1, -1):
                mm = re.match(pat, src[i])
                if mm:
                    name = mm.group(1) if where == mod else 'prop_' + mm.group(1)
                    break

    if name:
        failed.append(name)
        print('FAIL', name, flush=True)
        cfg = open(mod + '.cfg').read()
        cfg = cfg.replace('    ' + name + '\n', '')
        open(mod + '.cfg','w').write(cfg)
        continue

    if capped:
        prog = re.findall(r'Progress\((\d+)\).*?([\d,]+) distinct', out)
        print('CAPPED with no violation;', ('depth %s, %s distinct' % prog[-1]) if prog else '?', flush=True)
    else:
        m = re.search(r'(\d[\d,]*) distinct states found, 0 states left', out)
        print('CLEAN COMPLETE;', m.group(1) if m else '?', 'distinct', flush=True)
        if 'Error' in out and not m:
            print(out[-2000:])
    break

print('FAILED TOTAL', len(failed))
for f in failed: print('  ', f)
