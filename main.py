from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from bs4 import BeautifulSoup

# -------------------
# DB 설정
# -------------------
DATABASE_URL = "sqlite:///./ipo_schedule.db"

Base = declarative_base()
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class IPOSchedule(Base):
    __tablename__ = "ipo_schedules"
    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)
    listing_date = Column(Date, nullable=True)
    scraped_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# -------------------
# 크롤링 (타임아웃/헤더/예외 처리)
# -------------------
def scrape_ipo():
    try:
        url = "https://www.38.co.kr/html/fund/index.htm?o=k"
        resp = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; IPOBot/1.0)"}
        )
        resp.encoding = "euc-kr"
        soup = BeautifulSoup(resp.text, "html.parser")

        schedules = []
        table = soup.find("table", {"summary": "공모주 청약일정"})
        if not table:
            print("⚠️ 테이블을 찾지 못했습니다. 사이트 구조가 변경되었을 수 있습니다.")
            return

        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            company = cols[0].get_text(strip=True)
            date_range = cols[1].get_text(strip=True).split("~")
            try:
                start_date = datetime.strptime(date_range[0], "%Y.%m.%d").date()
                end_date = datetime.strptime(date_range[1], "%Y.%m.%d").date()
            except:
                continue

            listing_date = None
            if len(cols) > 2 and cols[2].get_text(strip=True):
                try:
                    listing_date = datetime.strptime(cols[2].get_text(strip=True), "%Y.%m.%d").date()
                except:
                    pass

            schedules.append({
                "company_name": company,
                "start_date": start_date,
                "end_date": end_date,
                "listing_date": listing_date
            })

        db = SessionLocal()
        db.query(IPOSchedule).delete()
        for s in schedules:
            db.add(IPOSchedule(**s))
        db.commit()
        db.close()

        print(f"[{datetime.now()}] {len(schedules)}개 일정 저장 완료")
    except Exception as e:
        # 어떤 에러가 나더라도 서버는 계속 살아있게
        print(f"❌ scrape_ipo 실패: {e}")

# -------------------
# FastAPI
# -------------------
app = FastAPI()
scheduler = BackgroundScheduler()

@app.on_event("startup")
def startup_event():
    # 서버 부팅을 절대 막지 않도록: 첫 실행을 '백그라운드로 즉시' 예약
    scheduler.add_job(scrape_ipo, "interval", weeks=1, next_run_time=datetime.utcnow())
    scheduler.start()
    print("✅ 스케줄러 시작: 주 1회 자동 크롤링(첫 실행은 비동기 즉시)")

@app.get("/")
def root():
    return {"ok": True, "msg": "IPO schedule API running. See /schedules"}

@app.get("/schedules")
def get_schedules(company: str = None):
    db = SessionLocal()
    query = db.query(IPOSchedule)
    if company:
        query = query.filter(IPOSchedule.company_name.contains(company))
    data = query.order_by(IPOSchedule.start_date).all()
    db.close()
    return [
        {
            "company_name": s.company_name,
            "start_date": s.start_date,
            "end_date": s.end_date,
            "listing_date": s.listing_date
        }
        for s in data
    ]
