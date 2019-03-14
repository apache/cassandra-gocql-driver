#!/usr/bin/env python3
# Helper script for managing AUTHORS file
import os
import argparse
import io


parser = argparse.ArgumentParser()
parser.add_argument('author', nargs='?')
parser.add_argument('--check', action='store_true', help='Return 0 if no changes are required')

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


def write_authors(f):
    for line in preamble:
        f.write(line)
    for author in sorted(authors, key=str.casefold):
        f.write(author)

if args.check:
    with open('AUTHORS', 'rb') as f:
        original = f.read()
    f = io.StringIO()
    write_authors(f)
    new = f.getvalue().encode('UTF-8')
    if original != new:
        print("Changes to AUTHORS file required")
        raise SystemExit(1)
else:
    with open('AUTHORS.tmp', 'w', encoding='UTF-8') as f:
        write_authors(f)
    os.rename('AUTHORS.tmp', 'AUTHORS')
