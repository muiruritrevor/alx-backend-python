# Aug 7

import asyncio
import aiosqlite

async def async_fetch_users(): 
    """
    Fetches all users.
    """
    async with aiosqlite.connect('users.db') as db:
        cursor = await db.execute("SELECT * FROM Users")
        users = await cursor.fetchall()
        await cursor.close()
        return users
    
async def async_fetch_older_users():
    """
    Fetches all users older than 40.
    """
    async with aiosqlite.connect('users.db') as db:
        cursor = await db.execute("SELECT * FROM Users WHERE age > 40")
        older_users = await cursor.fetchall()
        await cursor.close()
        return older_users

async def fetch_concurrently():
    """
    Concurrently interact with SQLite
    """
    users, older_users = await asyncio.gather(
        async_fetch_users(),
        async_fetch_older_users()
    )

    print("All Users:")
    for user in users:
        print(user)

    print("\nUsers Older Than 40:")
    for user in older_users:
        print(user)

# Run the concurrent fetch
asyncio.run(fetch_concurrently()) 


