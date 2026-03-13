import os, openpyxl

base = os.path.join('data','ab')
results = []
for entry in os.listdir(base):
    sub = os.path.join(base, entry)
    if not os.path.isdir(sub):
        continue
    links = os.path.join(sub, 'links_filtered.txt')
    if not os.path.isfile(links) or os.path.getsize(links) == 0:
        continue
    def exists_nonzero(fname):
        path = os.path.join(sub, fname)
        return os.path.isfile(path) and os.path.getsize(path) > 0
    has_gemini = exists_nonzero('gemini_extraction')
    has_glm = exists_nonzero('glm_extraction')
    has_cleaned = exists_nonzero('cleaned_pages.ndjson')
    if not (has_gemini or has_glm or has_cleaned):
        results.append(entry)

print(f'Found {len(results)} directories meeting criteria')
for r in results[:20]:
    print('-', r)

wb = openpyxl.Workbook()
ws = wb.active
ws.title = 'missing_extractions'
ws.append(['subdirectory'])
for r in results:
    ws.append([r])
out = 'ab_no_extraction.xlsx'
wb.save(out)
print('Saved', out)
