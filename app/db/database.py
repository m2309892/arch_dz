import os
from dotenv import load_dotenv

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
    AsyncEngine
)

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
        
        # Преобразуем postgresql:// в postgresql+asyncpg:// для async
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
            max_overflow=20,
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
    
    async def disconnect(self):
        if self.engine:
            await self.engine.dispose()
            print("Подключение к базе данных закрыто")
    
    def get_session(self) -> AsyncSession:
        if not self.session_maker:
            raise RuntimeError("База данных не подключена. Вызовите db.connect()")
        
        return self.session_maker()
    
    async def create_tables(self):
        """Создает все таблицы в базе данных."""
        from app.db.models import Base
        
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def drop_tables(self):
        """Удаляет все таблицы из базы данных."""
        from app.db.models import Base
        
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)


db = Database()
