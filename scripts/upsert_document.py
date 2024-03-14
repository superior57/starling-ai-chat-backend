from fire import Fire
from app.schema import Document
from app.db.session import SessionLocal
from app.api import crud
import asyncio

async def upsert_single_document(doc_url: str, doc_name: str):
    """
    Upserts a single SEC document into the database using its URL.
    """
    if not doc_url or not doc_url.startswith('http'):
        print("DOC_URL must be an http(s) based url value")
        return
    metadata_map = {
        "sec_document": {
            "cik": "0001318605",
            "year": 2024,
            "doc_type": "10-K",
            "company_name": doc_name,
            "company_ticker": doc_name,
            "accession_number": "0000950170-22-000796",
            "filed_as_of_date": "2024-03-06T17:21:45.5",
            "date_as_of_change": "2024-03-06T17:21:45.5",
            "period_of_report_date": "2024-03-06T17:21:45.5"
        }
    }
    doc = Document(url=doc_url, metadata_map=metadata_map)

    async with SessionLocal() as db:
        document = await crud.upsert_document_by_url(db, doc)
        print(f"Upserted document. Database ID:\n{document.id}")

async def upsert_multiple_document():
    documents = [
        "05_cultureaudits_jan2021_v04_264153eaf9",
        "07_gcra_sept2020_v01_b04718e3e0",
        "08_politicsofrage_jan2021_v02_59601149f0",
        "09_regulatorytrends_jan2021_v04_41e0c44683",
        "10_singapore_jan2021_v01_f013e5aadc",
        "11_predictivetech_jan2021_v02_ff7102f82e",
        "2018 Starling Compendium",
        "2019 Starling Compendium",
        "2020 Starling Compendium",
        "a_better_standard_of_care_starling_27f3295843",
        "Audit Firms Face Culture & Conduct Scrutiny",
        "Compendium 2023 Web Layout v24",
        "Compendium_2022_v23",
        "Deeper Dive - Feb2024 v12",
        "Deeper Dive - Renal Failure - Mar2023 v08",
        "Deeper Dive - The Costs of Misconduct - Oct2022 v09",
        "Deeper Dive - The Era of Accountability - Jan2023 v07",
        "Starling 2021 Compendium (PDF optimized)",
        "Three Lines of Defense V12"
    ]
    for doc in documents:
        await upsert_single_document(f"http://localhost:3000/pdfs/{doc}.pdf", doc)    


def main_upsert_single_document(doc_url: str):
    """
    Script to upsert a single document by URL. metada_map parameter will be empty dict ({})
    This script is useful when trying to use your own PDF files.
    """
    asyncio.run(upsert_single_document(doc_url))

if __name__ == "__main__":
    # Fire(main_upsert_single_document)
    Fire(upsert_multiple_document)
