import os
import asyncio
from dotenv import load_dotenv

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
    AsyncEngine
)
from sqlalchemy.pool import NullPool

load_dotenv()


class Database:
    
    def __init__(self):
        self.engine: AsyncEngine | None = None
        self.session_maker: async_sessionmaker[AsyncSession] | None = None
    
    async def connect(self):
        database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5432/video_analytics"
        )
        
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://",
                "postgresql+asyncpg://",
                1
            )
        
        self.engine = create_async_engine(
            database_url,
            echo=False,
            poolclass=NullPool,
        )
        
        self.session_maker = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False
        )
        print("Подключение к базе данных установлено")
    
    async def disconnect(self):
        if self.engine:
            try:
                await self.engine.dispose(close=True)
                print("Подключение к базе данных закрыто")
            except Exception as e:
                print(f"Ошибка при закрытии соединения с БД: {e}")
                try:
                    await self.engine.dispose()
                except:
                    pass
    
    def get_session(self) -> AsyncSession:
        if not self.session_maker:
            raise RuntimeError("База данных не подключена. Вызовите db.connect()")
        
        return self.session_maker()
    
    async def create_tables(self):
        from app.db.models import Base
        
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def drop_tables(self):
        from app.db.models import Base
        
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)


db = Database()
