#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 10:17:23 2017

@author: jlroo
"""


def initial_book(data, security_id, product):
	sec_desc_id = b'\x0148=' + security_id.encode() + b'\x01'
	msg_type = lambda e: b'35=X\x01' in e and sec_desc_id in e
	trade_type = lambda e: e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
	open_msg = lambda e: True if msg_type(e) and trade_type(e) else None
	temp = b'\x01279=NA\x0122=NA' + sec_desc_id + \
	       b"83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023="
	if product in "opt|options.py":
		top_order = 3
		prev_body = [temp + str(i).encode() for i in range(1, top_order + 1)]
		temp = temp.replace(b'\x01269=0', b'\x01269=1')
		prev_body = prev_body + [temp + str(i).encode() for i in range(1, top_order + 1)]
	if product in "fut|futures":
		top_order = 10
		prev_body = [temp + str(i).encode() for i in range(1, top_order + 1)]
		temp = temp.replace(b'\x01269=0', b'\x01269=1')
		prev_body = prev_body + [temp + str(i).encode() for i in range(1, top_order + 1)]
	msg = next(filter(open_msg, data), None)
	book_header = msg.split(b'\x01279')[0]
	book_end = b'\x0110' + msg.split(b'\x0110')[-1]
	msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
	msg_body = [b'\x01279' + e for e in msg_body if sec_desc_id in e and b'\x01276' not in e]
	msg_body = iter(filter(lambda e: trade_type(e), msg_body))
	# BOOK UPDATE
	bids, offers = __update__(prev_body, msg_body, sec_desc_id, top_order)
	book_body = bids + offers
	book_header += b''.join([e for e in book_body])
	book = book_header + book_end
	data.seek(0)
	return book


def build_book(prev_book, update_msg, security_id, top_order):
	sec_desc_id = b'\x0148=' + security_id.encode() + b'\x01'
	trade_type = lambda e: e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
	prev_body = prev_book.split(b'\x0110=')[0]
	prev_body = prev_body.split(b'\x01279')[1:]
	prev_body = [b'\x01279' + entry for entry in prev_body]
	book_header = update_msg.split(b'\x01279')[0]
	book_end = b'\x0110' + update_msg.split(b'\x0110')[-1]
	msg_body = update_msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
	msg_body = [b'\x01279' + e for e in msg_body if sec_desc_id in e and b'\x01276' not in e]
	msg_body = iter(filter(lambda e: trade_type(e), msg_body))
	# BOOK UPDATE
	bids, offers = __update__(prev_body, msg_body, sec_desc_id, top_order)
	book_body = bids + offers
	if book_body == prev_body:
		book = None
	else:
		book_header += b''.join([e for e in book_body])
		book = book_header + book_end
	return book


def __update__(book_body, msg_body, sec_desc_id, top_order):
    bids, offers = book_body[0:top_order], book_body[top_order:]
    for entry in msg_body:
        try:
            price_level = int(entry.split(b'\x011023=')[1])
            entry_type = int(entry[entry.find(b'\x01269=') + 5:entry.find(b'\x01269=') + 6])
            action_type = int(entry[entry.find(b'\x01279=') + 5:entry.find(b'\x01279=') + 6])
            temp = b'\x01279=NA\x0122=NA' + sec_desc_id + b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
            if entry_type == 0:  # BID tag 269= esh9[1]
                if action_type == 1:  # CHANGE 279=1
                    bids[price_level - 1] = entry
                elif action_type == 0:  # NEW tag 279=0
                    if price_level == top_order:
                        bids[top_order - 1] = entry
                    else:
                        bids.insert(price_level - 1, entry)
                        for i in range(price_level, top_order):
                            bids[i] = bids[i].replace(b'\x011023=' + str(i).encode(), b'\x011023=' + str(i + 1).encode())
                        bids.pop()
                else:  # b'\x01279=2' DELETE
                    delete = temp + str(top_order).encode()
                    if price_level == top_order:
                        bids[top_order - 1] = delete
                    else:
                        bids.pop(price_level - 1)
                        for i in range(price_level, top_order):
                            bids[i - 1] = bids[i - 1].replace(b'\x011023=' + str(i + 1).encode(), b'\x011023=' + str(i).encode())
                        bids.append(delete)
            else:  # OFFER tag 269=1
                if action_type == 1:  # CHANGE 279=1
                    offers[price_level - 1] = entry
                elif action_type == 0:  # NEW tag 279=0
                    if price_level == top_order:
                        offers[top_order - 1] = entry
                    else:
                        offers.insert(price_level - 1, entry)
                        for i in range(price_level, top_order):
                            offers[i] = offers[i].replace(b'\x011023=' + str(i).encode(), b'\x011023=' + str(i + 1).encode())
                        offers.pop()
                else:  # b'\x01279=2' DELETE
                    temp = temp.replace(b'\x01269=0', b'\x01269=1')
                    delete = temp + str(top_order).encode()
                    if price_level == top_order:
                        offers[top_order - 1] = delete
                    else:
                        offers.pop(price_level - 1)
                        for i in range(price_level, top_order):
                            offers[i - 1] = offers[i - 1].replace(b'\x011023=' + str(i + 1).encode(), b'\x011023=' + str(i).encode())
                        offers.append(delete)
        except StopIteration:
            continue
    return bids, offers
