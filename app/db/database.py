import asyncio
import logging
import os
from dotenv import load_dotenv

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
    AsyncEngine
)

load_dotenv()
logger = logging.getLogger(__name__)


class Database:
    
    def __init__(self):
        self.engine: AsyncEngine | None = None
        self.session_maker: async_sessionmaker[AsyncSession] | None = None
    
    async def connect(self):
        database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@127.0.0.1:5432/video_analytics"
        )
        if "@localhost:" in database_url:
            database_url = database_url.replace("@localhost:", "@127.0.0.1:")
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://",
                "postgresql+asyncpg://",
                1
            )
        
        self.engine = create_async_engine(
            database_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        
        self.session_maker = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False
        )
        print("Подключение к базе данных установлено")

    async def warmup(
        self,
        initial_delay: float = 3.0,
        retries: int = 15,
        delay: float = 2.0,
    ) -> None:
        from sqlalchemy import text

        await asyncio.sleep(initial_delay)
        last_err = None
        for attempt in range(retries):
            try:
                async with self.engine.begin() as conn:
                    await conn.execute(text("SELECT 1"))
                logger.info("БД warmup успешен")
                return
            except Exception as e:
                last_err = e
                logger.warning(
                    "БД warmup попытка %d/%d: %s. Пауза %.1f с...",
                    attempt + 1,
                    retries,
                    e,
                    delay,
                )
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
        raise last_err

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
