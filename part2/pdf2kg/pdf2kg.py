import os
import sys
import json
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple

from neo4j import GraphDatabase
from dotenv import load_dotenv
from docling.document_converter import DocumentConverter, InputFormat, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from hierarchical.postprocessor import ResultPostprocessor
from docling_core.types.doc import SectionHeaderItem, TextItem, TableItem

load_dotenv()

# ============================================
# 데이터 클래스
# ============================================

@dataclass
class TOCNode:
    """목차 노드"""
    toc_id: str
    title: str
    level: int
    parent_id: Optional[str]
    is_leaf: bool
    page_start: int = 0
    page_end: int = 0
    bbox: Optional[Tuple[float, float, float, float]] = None


@dataclass
class TextElement:
    """텍스트 요소"""
    element_id: str
    toc_id: str
    text: str
    page: int
    bbox: Tuple[float, float, float, float]


@dataclass
class TableElement:
    """테이블 요소"""
    element_id: str
    toc_id: str
    page: int
    bbox: Tuple[float, float, float, float]
    content: str


@dataclass
class DocumentStructure:
    """문서 구조 전체"""
    title: str
    pdf_path: str
    total_pages: int
    toc: List[TOCNode]
    texts: List[TextElement]
    tables: List[TableElement]


# ============================================
# 1단계: TOC 추출
# ============================================

def extract_hierarchical_toc(pdf_path: str) -> Tuple[List[TOCNode], Any]:
    """docling을 사용하여 계층적 목차(Table of Contents) 구조 추출"""
    print(f"\n[1/3] TOC 추출 중: {pdf_path}")

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = False

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
                backend=PyPdfiumDocumentBackend
            )
        }
    )

    print("   PDF 변환 중...")
    result = converter.convert(pdf_path)

    print("   계층 구조 처리 중...")
    postprocessor = ResultPostprocessor(result)
    postprocessor.process()

    toc_nodes = []
    parent_stack = []
    toc_idx = 0

    for item, prov in result.document.iterate_items():
        if isinstance(item, SectionHeaderItem):
            level = getattr(item, 'level', 1)
            title = item.text.strip()
            if not title or len(title) < 2:
                continue

            toc_id = f"toc_{toc_idx:03d}"

            while parent_stack and parent_stack[-1]['level'] >= level:
                parent_stack.pop()

            parent_id = parent_stack[-1]['toc_id'] if parent_stack else None

            page_num = 1
            bbox = None

            if hasattr(item, 'prov') and item.prov:
                item_prov = item.prov
                if hasattr(item_prov, '__len__') and len(item_prov) > 0:
                    first_prov = item_prov[0]
                    if hasattr(first_prov, 'page_no'):
                        page_num = first_prov.page_no + 1
                    if hasattr(first_prov, 'bbox'):
                        prov_bbox = first_prov.bbox
                        if hasattr(prov_bbox, 'l'):
                            bbox = (prov_bbox.l, prov_bbox.t, prov_bbox.r, prov_bbox.b)
                        elif isinstance(prov_bbox, (list, tuple)) and len(prov_bbox) == 4:
                            bbox = tuple(prov_bbox)

            toc_node = TOCNode(
                toc_id=toc_id,
                title=title,
                level=level,
                parent_id=parent_id,
                is_leaf=True,
                page_start=page_num,
                bbox=bbox
            )
            toc_nodes.append(toc_node)

            if parent_id:
                for node in toc_nodes:
                    if node.toc_id == parent_id:
                        node.is_leaf = False
                        break

            parent_stack.append({'toc_id': toc_id, 'level': level})
            toc_idx += 1

    level_counts = {}
    for node in toc_nodes:
        level_counts[node.level] = level_counts.get(node.level, 0) + 1

    print(f"   TOC 노드 {len(toc_nodes)}개 추출")
    print(f"   레벨 분포: {dict(sorted(level_counts.items()))}")

    return toc_nodes, result


# ============================================
# 2단계: 요소 추출 및 매핑
# ============================================

def extract_and_map_elements(docling_result, toc_nodes: List[TOCNode]) -> Tuple[List[TextElement], List[TableElement]]:
    """docling iterate_items를 사용하여 텍스트/테이블 요소를 추출하고 TOC에 매핑"""
    print(f"\n[2/3] 요소 추출 중...")

    all_texts = []
    all_tables = []

    text_idx = 0
    table_idx = 0

    # TOC를 순서대로 매칭 (제목 중복 문제 해결)
    toc_iterator = iter(toc_nodes)
    toc_map_by_title_page = {}  # {(title, page): toc_node}

    for node in toc_nodes:
        key = (node.title, node.page_start)
        if key not in toc_map_by_title_page:
            toc_map_by_title_page[key] = []
        toc_map_by_title_page[key].append(node)

    active_toc_stack = {}
    current_toc = None

    print(f"   순차 처리 중...")

    item_count = 0
    for item, prov in docling_result.document.iterate_items():
        item_count += 1

        if isinstance(item, SectionHeaderItem):
            title = item.text.strip()

            # 페이지 번호 추출
            page_num = 1
            if hasattr(item, 'prov') and item.prov:
                item_prov = item.prov
                if hasattr(item_prov, '__len__') and len(item_prov) > 0:
                    first_prov = item_prov[0]
                    if hasattr(first_prov, 'page_no'):
                        page_num = first_prov.page_no + 1

            # 제목과 페이지로 TOC 찾기 (중복 제목 처리)
            key = (title, page_num)
            if key in toc_map_by_title_page and toc_map_by_title_page[key]:
                toc_node = toc_map_by_title_page[key].pop(0)  # 순서대로 하나씩 사용
                level = toc_node.level

                active_toc_stack = {lv: node for lv, node in active_toc_stack.items() if lv < level}
                active_toc_stack[level] = toc_node

                current_toc = toc_node

        elif isinstance(item, TextItem):
            if current_toc is None:
                continue

            text = item.text.strip()
            if not text or len(text) < 5:
                continue

            page_num = 1
            bbox_tuple = (0, 0, 0, 0)

            if hasattr(item, 'prov') and item.prov:
                item_prov = item.prov
                if hasattr(item_prov, '__len__') and len(item_prov) > 0:
                    first_prov = item_prov[0]
                    if hasattr(first_prov, 'page_no'):
                        page_num = first_prov.page_no + 1
                    if hasattr(first_prov, 'bbox'):
                        prov_bbox = first_prov.bbox
                        if hasattr(prov_bbox, 'l'):
                            bbox_tuple = (prov_bbox.l, prov_bbox.t, prov_bbox.r, prov_bbox.b)

            all_texts.append(TextElement(
                element_id=f"text_{text_idx:04d}",
                toc_id=current_toc.toc_id,
                text=text,
                page=page_num,
                bbox=bbox_tuple
            ))

            text_idx += 1

        elif isinstance(item, TableItem):
            if current_toc is None:
                continue

            page_num = 1
            bbox_tuple = (0, 0, 0, 0)

            if hasattr(item, 'prov') and item.prov:
                item_prov = item.prov
                if hasattr(item_prov, '__len__') and len(item_prov) > 0:
                    first_prov = item_prov[0]
                    if hasattr(first_prov, 'page_no'):
                        page_num = first_prov.page_no + 1
                    if hasattr(first_prov, 'bbox'):
                        prov_bbox = first_prov.bbox
                        if hasattr(prov_bbox, 'l'):
                            bbox_tuple = (prov_bbox.l, prov_bbox.t, prov_bbox.r, prov_bbox.b)

            table_text = ""
            try:
                if hasattr(item, 'export_to_markdown'):
                    table_text = item.export_to_markdown(doc=docling_result.document)

            except Exception as e:
                pass

            if not table_text:
                table_text = f"[Table {table_idx}]"

            all_tables.append(TableElement(
                element_id=f"table_{table_idx:04d}",
                toc_id=current_toc.toc_id,
                page=page_num,
                bbox=bbox_tuple,
                content=table_text
            ))

            table_idx += 1

    print(f"   텍스트 {len(all_texts)}개, 표 {len(all_tables)}개 추출")
    print(f"   {item_count}개 항목(헤더, 텍스트/테이블 요소) 처리")

    return all_texts, all_tables


# ============================================
# 3단계: 통계 계산
# ============================================

def calculate_toc_stats(toc_nodes: List[TOCNode], texts: List[TextElement],
                       tables: List[TableElement], pdf_path: str):
    """TOC별 통계 계산 및 요약 저장"""
    print(f"\n[3/3] 통계 계산 중...")

    # output 폴더 생성
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / f"{Path(pdf_path).stem}_toc_structure.txt"

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write(f"목차 구조 및 콘텐츠 분포: {Path(pdf_path).name}\n")
        f.write("="*80 + "\n\n")

        f.write("목차 계층 구조:\n")
        f.write("-"*80 + "\n")

        for node in toc_nodes:
            indent = "  " * (node.level - 1)
            t_count = sum(1 for t in texts if t.toc_id == node.toc_id)
            tb_count = sum(1 for tb in tables if tb.toc_id == node.toc_id)
            total = t_count + tb_count

            leaf_mark = "[L]" if node.is_leaf else "[P]"
            content_info = f"[T:{t_count} Tb:{tb_count}]" if total > 0 else ""

            f.write(f"{indent}{leaf_mark} [{node.toc_id}] {node.title} (p.{node.page_start}) {content_info}\n")

        f.write("\n\n")

        content_tocs = sum(1 for node in toc_nodes
                          if sum(1 for t in texts if t.toc_id == node.toc_id) +
                             sum(1 for tb in tables if tb.toc_id == node.toc_id) > 0)

        f.write(f"\nTotal: {len(toc_nodes)} TOC nodes, {content_tocs} with content\n")
        f.write(f"Elements: {len(texts)} texts, {len(tables)} tables\n")

    print(f"   TOC별 콘텐츠 구조 저장: {output_path}")


# ============================================
# 메인 처리 함수
# ============================================

def process_pdf(pdf_path: str) -> DocumentStructure:
    """PDF를 처리하고 구조를 추출"""
    toc_nodes, docling_result = extract_hierarchical_toc(pdf_path)

    if not toc_nodes:
        print("\n경고: TOC를 추출할 수 없습니다!")
        return None

    texts, tables = extract_and_map_elements(docling_result, toc_nodes)

    calculate_toc_stats(toc_nodes, texts, tables, pdf_path)

    doc_structure = DocumentStructure(
        title=Path(pdf_path).stem,
        pdf_path=pdf_path,
        total_pages=len(docling_result.document.pages),
        toc=toc_nodes,
        texts=texts,
        tables=tables
    )

    print("\n" + "="*70)
    print("처리 완료")
    print("="*70)
    print(f"   TOC: {len(toc_nodes)}개")
    print(f"   텍스트: {len(texts)}개")
    print(f"   표: {len(tables)}개")

    return doc_structure


# ============================================
# JSON 저장
# ============================================

def save_to_json(doc_structure: DocumentStructure, output_path: str):
    """문서 구조를 JSON으로 저장"""
    data = {
        "title": doc_structure.title,
        "pdf_path": doc_structure.pdf_path,
        "total_pages": doc_structure.total_pages,
        "toc": [asdict(node) for node in doc_structure.toc],
        "texts": [asdict(elem) for elem in doc_structure.texts[:100]],
        "tables": [asdict(elem) for elem in doc_structure.tables]
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nJSON 저장: {output_path}")


# ============================================
# Neo4j 저장
# ============================================

def save_to_neo4j(doc_structure: DocumentStructure, uri, user, password):
    """문서 구조를 Neo4j에 저장"""
    print(f"\nNeo4j 저장 중: {uri}")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        # 1. 기존 데이터 삭제
        driver.execute_query(
            "MATCH (n) DETACH DELETE n",
            database_="neo4j"
        )
        print("   step 1 - 기존 데이터 삭제")

        # 2. Document 노드 생성
        driver.execute_query(
            """
            CREATE (d:Document {
                title: $title,
                pdf_path: $pdf_path,
                total_pages: $total_pages
            })
            """,
            title=doc_structure.title,
            pdf_path=doc_structure.pdf_path,
            total_pages=doc_structure.total_pages,
            database_="neo4j"
        )
        print(f"   step 2 - Document 노드 생성")

        # 3. TOC 노드 생성
        level_labels = {1: "Level1", 2: "Level2", 3: "Level3", 4: "Level4", 5: "Level5"}

        def create_toc_nodes(tx, toc_list):
            """트랜잭션 함수: TOC 노드 생성"""
            for node in toc_list:
                label = level_labels.get(node.level, f"Level{node.level}")
                query = f"""
                    CREATE (t:{label}:TOC {{
                        toc_id: $toc_id,
                        title: $title,
                        level: $level,
                        parent_id: $parent_id,
                        is_leaf: $is_leaf,
                        page_start: $page_start
                    }})
                """
                tx.run(query,
                       toc_id=node.toc_id,
                       title=node.title,
                       level=node.level,
                       parent_id=node.parent_id,
                       is_leaf=node.is_leaf,
                       page_start=node.page_start)
            return len(toc_list)

        with driver.session(database="neo4j") as session:
            toc_count = session.execute_write(create_toc_nodes, doc_structure.toc)
            print(f"   step 3 - TOC 노드 {toc_count}개 생성")

        # 4. TOC 관계 생성
        driver.execute_query(
            """
            MATCH (parent:TOC), (child:TOC)
            WHERE child.parent_id = parent.toc_id
            CREATE (parent)-[:HAS_CHILD]->(child)
            """,
            database_="neo4j"
        )
        print(f"   step 4 - TOC 관계 생성")

        # 5. Chunk 노드 생성
        chunks_to_create = []
        for node in doc_structure.toc:
            toc_texts = [t for t in doc_structure.texts if t.toc_id == node.toc_id]
            toc_tables = [t for t in doc_structure.tables if t.toc_id == node.toc_id]

            if len(toc_texts) + len(toc_tables) > 0:
                content_parts = [t.text for t in toc_texts]
                content_parts.extend([f"[Table {t.element_id}]\n{t.content}" for t in toc_tables])
                combined_text = "\n\n".join(content_parts)

                chunks_to_create.append({
                    "chunk_id": f"chunk_{len(chunks_to_create):04d}",
                    "toc_id": node.toc_id,
                    "content": combined_text,
                    "text_count": len(toc_texts),
                    "table_count": len(toc_tables)
                })

        def create_chunk_nodes(tx, chunks):
            """트랜잭션 함수: Chunk 노드 생성"""
            for chunk in chunks:
                tx.run("""
                    CREATE (c:Chunk {
                        chunk_id: $chunk_id,
                        toc_id: $toc_id,
                        content: $content,
                        text_count: $text_count,
                        table_count: $table_count
                    })
                """, **chunk)
            return len(chunks)

        with driver.session(database="neo4j") as session:
            chunk_count = session.execute_write(create_chunk_nodes, chunks_to_create)
            print(f"   step 5 - Chunk 노드 {chunk_count}개 생성")

        # 6. TOC-Chunk 관계 생성
        driver.execute_query(
            """
            MATCH (toc:TOC), (c:Chunk)
            WHERE c.toc_id = toc.toc_id
            CREATE (toc)-[:HAS_CHUNK]->(c)
            """,
            database_="neo4j"
        )
        print(f"   step 6 - TOC-Chunk 관계 생성")

        # 7. TextElement 노드 생성 (배치)
        def create_text_elements(tx, texts_batch):
            """트랜잭션 함수: TextElement 배치 생성"""
            tx.run("""
                UNWIND $texts AS text
                CREATE (t:TextElement {
                    element_id: text.element_id,
                    toc_id: text.toc_id,
                    content: text.text,
                    page: text.page
                })
            """, texts=texts_batch)

        with driver.session(database="neo4j") as session:
            for i in range(0, len(doc_structure.texts), 100):
                batch = doc_structure.texts[i:i+100]
                batch_data = [{
                    "element_id": t.element_id,
                    "toc_id": t.toc_id,
                    "text": t.text,
                    "page": t.page
                } for t in batch]
                session.execute_write(create_text_elements, batch_data)
        print(f"   step 7 - TextElement 노드 {len(doc_structure.texts)}개 생성")

        # 8. TableElement 노드 생성
        if doc_structure.tables:
            def create_table_elements(tx, tables_list):
                """트랜잭션 함수: TableElement 생성"""
                for table in tables_list:
                    tx.run("""
                        CREATE (t:TableElement {
                            element_id: $element_id,
                            toc_id: $toc_id,
                            page: $page,
                            content: $content
                        })
                    """, element_id=table.element_id,
                         toc_id=table.toc_id,
                         page=table.page,
                         content=table.content)

            with driver.session(database="neo4j") as session:
                session.execute_write(create_table_elements, doc_structure.tables)
            print(f"   step 8 - TableElement 노드 {len(doc_structure.tables)}개 생성")

        # 9. Chunk-Element 관계 생성
        text_rels_result = driver.execute_query(
            """
            MATCH (c:Chunk), (t:TextElement)
            WHERE t.toc_id = c.toc_id
            CREATE (c)-[:HAS_ELEMENT]->(t)
            RETURN count(*) as cnt
            """,
            database_="neo4j"
        )
        text_rels = text_rels_result.records[0]["cnt"]

        table_rels_result = driver.execute_query(
            """
            MATCH (c:Chunk), (t:TableElement)
            WHERE t.toc_id = c.toc_id
            CREATE (c)-[:HAS_ELEMENT]->(t)
            RETURN count(*) as cnt
            """,
            database_="neo4j"
        )
        table_rels = table_rels_result.records[0]["cnt"]

        print(f"   step 9 - Chunk-Element 관계 생성: 텍스트 {text_rels}개, 표 {table_rels}개")

        # 10. Document-TOC 관계 생성
        driver.execute_query(
            """
            MATCH (d:Document), (toc:TOC)
            WHERE toc.parent_id IS NULL
            CREATE (d)-[:HAS_TOC]->(toc)
            """,
            database_="neo4j"
        )

    finally:
        driver.close()

    print(f"\nNeo4j 저장 완료!")


# ============================================
# 메인
# ============================================

if __name__ == "__main__":
    print("=" * 50)
    print("PDF 지식 그래프 추출 - (1) 목차 및 텍스트")
    print("=" * 50)

    pdf_path = "aibrief.pdf"

    if not Path(pdf_path).exists():
        print(f"오류: {pdf_path} 파일을 찾을 수 없습니다")
        sys.exit(1)

    doc_structure = process_pdf(pdf_path)

    if doc_structure:
        # output 폴더 생성
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        output_path = output_dir / f"{Path(pdf_path).stem}_structure.json"
        save_to_json(doc_structure, str(output_path))

        NEO4J_URI = os.getenv("NEO4J_URI")
        NEO4J_USER = os.getenv("NEO4J_USERNAME")
        NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

        if NEO4J_URI and NEO4J_USER and NEO4J_PASSWORD:
            save_to_neo4j(doc_structure, uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)