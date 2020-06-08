import os

folder = 'seafarers'
for filename in os.listdir(folder):
    in_filename = os.path.join(folder, filename)
    if not os.path.isfile(in_filename): continue    
    out_filename = in_filename.replace('.json', '.html')
    os.rename(in_filename, out_filename)
    print(f'{in_filename} renamed to {out_filename}')
