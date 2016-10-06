#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging; logging.basicConfig(level=logging.INFO)
import aiomysql
from aiohttp.pytest_plugin import loop


@asyncio.coroutine
def create_pool(loop,**kw):
    logging.info('create database connection pool...')
    global _pool 
    _pool = yield from aiomysql.create_pool(
                                            host=kw.get('host'),'localhost'),
                                            port=kw.get('port',3306),
                                            user=kw['user'],
                                            password=kw['password'],
                                            db=kw['db'],
                                            charset=kw.get('charset','utf-8'),
                                            autocommit=kw.get('autocommit',10),
                                            minsize=kw.get('minsize',1),
                                            loop=loop)
                                            
@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global _pool 
    with (yield from _pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?','%s'),args or ()))
        if size:
            rs=yield from cur.fetchmany(size)
            
        else:
            rs=yield from cur.fetchall()
            
        yield from cur.close()
        logging.info('rows returned:%s' % len(rs))
        return rs
    
@asyncio.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected
