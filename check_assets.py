import asyncio
from sqlalchemy import select
from app.clients import db as db_client
from app.models.market_data import MarketInstrument

async def main():
    async with db_client.async_session() as db:
        rows = (await db.execute(select(MarketInstrument.symbol, MarketInstrument.asset_type))).all()
        for r in rows:
            print(f'{r[0]}: {r[1]}')

asyncio.run(main())
