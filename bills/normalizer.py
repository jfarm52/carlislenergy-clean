"""
Normalization Service for Bill Text Extraction
===============================================
Converts PDFs, images, and spreadsheets to clean text for LLM processing.
"""

import os
import logging
from typing import Tuple, Dict, Any, Optional
from dataclasses import dataclass

import pymupdf  # PyMuPDF 1.26+ uses pymupdf, not fitz
from PIL import Image
import pytesseract
# Lazy import pandas - only needed for Excel/CSV, not for PDFs
# import pandas as pd  # Moved to lazy import in _normalize_spreadsheet

logger = logging.getLogger(__name__)

@dataclass
class NormalizationResult:
    """Result of file normalization."""
    text: str
    metadata: Dict[str, Any]
    success: bool
    error: Optional[str] = None


class NormalizationService:
    """
    Normalizes various file types (PDF, images, Excel/CSV) to plain text.
    
    Detection order:
    1. PDF with native text -> PyMuPDF extraction
    2. PDF without text (scanned) -> OCR via pytesseract
    3. Images (jpg, png, tiff, etc) -> OCR via pytesseract
    4. Excel/CSV -> pandas read + convert to text
    """
    
    PDF_EXTENSIONS = {'.pdf'}
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.heic'}
    SPREADSHEET_EXTENSIONS = {'.xlsx', '.xls', '.csv'}
    
    MIN_TEXT_LENGTH_FOR_NATIVE = 100
    OCR_CONFIG = '--oem 3 --psm 6'
    
    def __init__(self, dpi: int = 200):
        """
        Initialize the normalization service.
        
        Args:
            dpi: DPI for PDF to image conversion (for OCR fallback)
        """
        self.dpi = dpi
    
    def normalize(self, file_path: str) -> NormalizationResult:
        """
        Normalize a file to text.
        
        Args:
            file_path: Path to the file to normalize
            
        Returns:
            NormalizationResult with text, metadata, and status
        """
        if not os.path.exists(file_path):
            return NormalizationResult(
                text="",
                metadata={},
                success=False,
                error=f"File not found: {file_path}"
            )
        
        ext = os.path.splitext(file_path)[1].lower()
        file_size = os.path.getsize(file_path)
        
        try:
            if ext in self.PDF_EXTENSIONS:
                return self._normalize_pdf(file_path, file_size)
            elif ext in self.IMAGE_EXTENSIONS:
                return self._normalize_image(file_path, file_size)
            elif ext in self.SPREADSHEET_EXTENSIONS:
                return self._normalize_spreadsheet(file_path, file_size, ext)
            else:
                return NormalizationResult(
                    text="",
                    metadata={"file_size": file_size, "extension": ext},
                    success=False,
                    error=f"Unsupported file type: {ext}"
                )
        except Exception as e:
            logger.exception(f"Error normalizing file {file_path}")
            return NormalizationResult(
                text="",
                metadata={"file_size": file_size, "extension": ext},
                success=False,
                error=str(e)
            )
    
    def _normalize_pdf(self, file_path: str, file_size: int, skip_ocr: bool = False) -> NormalizationResult:
        """
        Extract text from PDF.
        
        Args:
            file_path: Path to PDF
            file_size: File size in bytes
            skip_ocr: If True, return native text even if insufficient (let caller decide)
        """
        doc = pymupdf.open(file_path)
        page_count = len(doc)
        
        native_text_parts = []
        for page in doc:
            text = page.get_text("text")
            if text:
                native_text_parts.append(text)
        
        native_text = "\n".join(native_text_parts)
        doc.close()
        
        native_char_count = len(native_text.strip())
        has_sufficient_text = native_char_count >= self.MIN_TEXT_LENGTH_FOR_NATIVE
        
        # If we have sufficient native text OR caller wants to skip OCR, return native
        if has_sufficient_text or skip_ocr:
            method = "pdf_native" if has_sufficient_text else "pdf_native_sparse"
            logger.info(f"PDF native text: {native_char_count} chars (method={method}, skip_ocr={skip_ocr})")
            return NormalizationResult(
                text=native_text,
                metadata={
                    "method": method,
                    "pages": page_count,
                    "file_size": file_size,
                    "confidence": 1.0 if has_sufficient_text else 0.5,
                    "char_count": native_char_count,
                    "has_sufficient_text": has_sufficient_text,
                },
                success=True
            )
        
        logger.info(f"PDF has insufficient native text ({native_char_count} chars), falling back to OCR")
        return self._ocr_pdf(file_path, page_count, file_size)
    
    def normalize_native_only(self, file_path: str) -> NormalizationResult:
        """
        Extract ONLY native/embedded text from PDF - NO OCR.
        Use this for fast first-pass extraction before deciding if OCR is needed.
        """
        if not os.path.exists(file_path):
            return NormalizationResult(
                text="",
                metadata={},
                success=False,
                error=f"File not found: {file_path}"
            )
        
        ext = os.path.splitext(file_path)[1].lower()
        file_size = os.path.getsize(file_path)
        
        if ext in self.PDF_EXTENSIONS:
            return self._normalize_pdf(file_path, file_size, skip_ocr=True)
        elif ext in self.IMAGE_EXTENSIONS:
            # Images have no native text - return empty but success=True so caller can decide
            return NormalizationResult(
                text="",
                metadata={"method": "image_no_native", "file_size": file_size},
                success=True,
                error=None
            )
        elif ext in self.SPREADSHEET_EXTENSIONS:
            # Spreadsheets always have native text
            return self._normalize_spreadsheet(file_path, file_size, ext)
        else:
            return NormalizationResult(
                text="",
                metadata={"file_size": file_size, "extension": ext},
                success=False,
                error=f"Unsupported file type: {ext}"
            )
    
    def _ocr_pdf(self, file_path: str, page_count: int, file_size: int) -> NormalizationResult:
        """OCR a PDF by converting pages to images."""
        doc = pymupdf.open(file_path)
        ocr_text_parts = []
        
        mat = pymupdf.Matrix(self.dpi / 72, self.dpi / 72)
        
        for page_num, page in enumerate(doc):
            try:
                pix = page.get_pixmap(matrix=mat)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text = pytesseract.image_to_string(img, config=self.OCR_CONFIG)
                ocr_text_parts.append(f"--- Page {page_num + 1} ---\n{text}")
            except Exception as e:
                logger.warning(f"OCR failed for page {page_num + 1}: {e}")
                ocr_text_parts.append(f"--- Page {page_num + 1} ---\n[OCR Error]")
        
        doc.close()
        ocr_text = "\n\n".join(ocr_text_parts)
        
        return NormalizationResult(
            text=ocr_text,
            metadata={
                "method": "pdf_ocr",
                "pages": page_count,
                "file_size": file_size,
                "confidence": 0.85,
                "char_count": len(ocr_text)
            },
            success=True
        )
    
    def _normalize_image(self, file_path: str, file_size: int) -> NormalizationResult:
        """Extract text from an image using OCR."""
        try:
            img = Image.open(file_path)
            if img.mode == 'RGBA':
                img = img.convert('RGB')
            
            text = pytesseract.image_to_string(img, config=self.OCR_CONFIG)
            
            return NormalizationResult(
                text=text,
                metadata={
                    "method": "image_ocr",
                    "pages": 1,
                    "file_size": file_size,
                    "image_size": f"{img.width}x{img.height}",
                    "confidence": 0.80,
                    "char_count": len(text)
                },
                success=True
            )
        except Exception as e:
            logger.exception(f"Image OCR failed: {e}")
            return NormalizationResult(
                text="",
                metadata={"file_size": file_size, "method": "image_ocr"},
                success=False,
                error=str(e)
            )
    
    def _normalize_spreadsheet(self, file_path: str, file_size: int, ext: str) -> NormalizationResult:
        """Convert spreadsheet to text representation."""
        try:
            # Lazy import pandas - only needed for Excel/CSV, avoids numpy dependency for PDF processing
            import pandas as pd
            
            if ext == '.csv':
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            text_parts = []
            text_parts.append(f"Columns: {', '.join(df.columns.astype(str))}")
            text_parts.append(f"Rows: {len(df)}")
            text_parts.append("")
            text_parts.append(df.to_string(index=False, max_rows=500))
            
            text = "\n".join(text_parts)
            
            return NormalizationResult(
                text=text,
                metadata={
                    "method": "spreadsheet",
                    "pages": 1,
                    "file_size": file_size,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "confidence": 1.0,
                    "char_count": len(text)
                },
                success=True
            )
        except Exception as e:
            logger.exception(f"Spreadsheet parsing failed: {e}")
            return NormalizationResult(
                text="",
                metadata={"file_size": file_size, "method": "spreadsheet"},
                success=False,
                error=str(e)
            )
    
    def detect_file_type(self, file_path: str) -> str:
        """
        Detect the type of file for processing.
        
        Returns:
            One of: 'pdf_native', 'pdf_scanned', 'image', 'spreadsheet', 'unknown'
        """
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext in self.SPREADSHEET_EXTENSIONS:
            return 'spreadsheet'
        elif ext in self.IMAGE_EXTENSIONS:
            return 'image'
        elif ext in self.PDF_EXTENSIONS:
            try:
                doc = pymupdf.open(file_path)
                text_len = sum(len(page.get_text("text")) for page in doc)
                doc.close()
                return 'pdf_native' if text_len >= self.MIN_TEXT_LENGTH_FOR_NATIVE else 'pdf_scanned'
            except:
                return 'unknown'
        return 'unknown'
