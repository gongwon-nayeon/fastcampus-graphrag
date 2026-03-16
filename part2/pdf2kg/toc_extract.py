from dataclasses import dataclass
from typing import Optional, Tuple

from docling.document_converter import DocumentConverter, InputFormat, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from hierarchical.postprocessor import ResultPostprocessor
from docling_core.types.doc import SectionHeaderItem


@dataclass
class TOCNode:
    """목차 노드"""
    toc_id: str
    title: str
    level: int
    parent_id: Optional[str]
    is_leaf: bool
    page_start: int = 0
    bbox: Optional[Tuple[float, float, float, float]] = None


def toc_extract_tester(pdf_path: str):
    print("="*80)
    print("Docling > ResultPostprocessor > ToC 추출 동작 테스트")
    print("="*80)

    print(f"\nPDF 파일: {pdf_path}")

    print("\n[1단계] docling - PDF 변환 중...")
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

    result = converter.convert(pdf_path)
    print(f"변환 완료")

    print("\ndocling-hierarchical-pdf 실행 전 상태")
    print("-"*80)

    section_headers_before = []
    for item, prov in result.document.iterate_items():
        if isinstance(item, SectionHeaderItem):
            section_headers_before.append({
                'text': item.text.strip()[:50],
                'level': getattr(item, 'level', None),
                'has_level_attr': hasattr(item, 'level'),
                'type': type(item).__name__
            })

    print(f"섹션 헤더 개수: {len(section_headers_before)}개")
    print("\n처음 5개 섹션 헤더:")
    for i, header in enumerate(section_headers_before[:5], 1):
        print(f"  {i}. \"{header['text']}\"")
        print(f"     - level 값: {header['level']}")

    print("\n[2단계] docling-hierarchical-pdf - ResultPostprocessor 실행 중...")
    postprocessor = ResultPostprocessor(result)
    postprocessor.process()

    print("\nResultPostprocessor 실행 후 상태")
    print("-"*80)

    section_headers_after = []
    for item, prov in result.document.iterate_items():
        if isinstance(item, SectionHeaderItem):
            section_headers_after.append({
                'text': item.text.strip()[:50],
                'level': getattr(item, 'level', None),
                'has_level_attr': hasattr(item, 'level'),
                'type': type(item).__name__
            })

    print(f"섹션 헤더 개수: {len(section_headers_after)}개")
    print("\n처음 5개 섹션 헤더:")
    for i, header in enumerate(section_headers_after[:5], 1):
        print(f"  {i}. \"{header['text']}\"")
        print(f"     - level 값: {header['level']}")

    print("\n[변화 분석]")
    print("="*80)

    level_distribution = {}
    for header in section_headers_after:
        if header['level'] is not None:
            level = header['level']
            level_distribution[level] = level_distribution.get(level, 0) + 1

    print(f"\nlevel 분포:")
    for level in sorted(level_distribution.keys()):
        count = level_distribution[level]
        print(f"   - Level {level}: {count}개")

    for i in range(min(20, len(section_headers_before))):
        before = section_headers_before[i]
        after = section_headers_after[i]

        print(f"\n{i+1}. \"{before['text']}\"")

        if before['level'] != after['level']:
            print(f"   level 변화: {before['level']} → {after['level']} ⭐")
        else:
            print(f"   level: {after['level']} (변화 없음)")


    print("\n\n[3단계] level을 기반으로 parent 관계 설정")
    print("="*80)

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

            # 현재 레벨보다 같거나 깊은 레벨을 스택에서 제거
            while parent_stack and parent_stack[-1]['level'] >= level:
                parent_stack.pop()

            # 가장 가까운 상위 레벨이 부모
            parent_id = parent_stack[-1]['toc_id'] if parent_stack else None

            page_num = 1
            bbox = None

            if hasattr(item, 'prov') and item.prov:
                item_prov = item.prov
                if hasattr(item_prov, '__len__') and len(item_prov) > 0:
                    first_prov = item_prov[0]
                    if hasattr(first_prov, 'page_no'):
                        page_num = first_prov.page_no + 1 # +1 해서 실제 페이지 번호
                    if hasattr(first_prov, 'bbox'): # (left, top, right, bottom)
                        prov_bbox = first_prov.bbox
                        if hasattr(prov_bbox, 'l'):
                            bbox = (prov_bbox.l, prov_bbox.t, prov_bbox.r, prov_bbox.b)
                        elif isinstance(prov_bbox, (list, tuple)) and len(prov_bbox) == 4:
                            bbox = tuple(prov_bbox)

            # TOC 노드 생성
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

            # 부모의 is_leaf를 False로 변경
            if parent_id:
                for node in toc_nodes:
                    if node.toc_id == parent_id:
                        node.is_leaf = False
                        break

            # 현재 노드를 스택에 추가
            parent_stack.append({'toc_id': toc_id, 'level': level})
            toc_idx += 1

    print(f"\n총 {len(toc_nodes)}개 TOC 노드 생성")
    print(toc_nodes[:5])

    # 계층 구조 출력 (처음 20개)
    print("\n계층 구조 (처음 20개):")
    print("-"*80)
    for i, node in enumerate(toc_nodes[:20], 1):
        indent = "  " * (node.level - 1)
        leaf_mark = "[L]" if node.is_leaf else "[P]"
        parent_info = f" (부모: {node.parent_id})" if node.parent_id else " (최상위)"
        print(f"{indent}{leaf_mark} [{node.toc_id}] Lv{node.level} - \"{node.title[:40]}\"{parent_info}")

if __name__ == "__main__":
    pdf_path = "aibrief.pdf"

    toc_extract_tester(pdf_path)
