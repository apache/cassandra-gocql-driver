#!/usr/bin/env python3
# Helper script for managing AUTHORS file
import os
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('author', nargs='?')

args = parser.parse_args()

preamble = []
authors = []

with open('AUTHORS', 'r', encoding='UTF-8') as f:
    for line in f:
        if line.startswith('#') or line.strip() == '':
            preamble.append(line)
            continue
        authors.append(line)

if args.author:
    authors.append(args.author + '\n')

with open('AUTHORS.tmp', 'w', encoding='UTF-8') as f:
    for line in preamble:
        f.write(line)
    for author in sorted(authors, key=str.casefold):
        f.write(author)

os.rename('AUTHORS.tmp', 'AUTHORS')
