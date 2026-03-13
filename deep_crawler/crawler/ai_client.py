"""
AI Client Abstract Base Class

Provides unified interface for GLM and Gemini extraction clients.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from pathlib import Path


class AIClient(ABC):
    """
    Abstract base class for AI extraction clients.
    Provides unified interface for GLM and Gemini implementations.
    """

    @abstractmethod
    def extract(
        self,
        pages: List[Dict[str, str]],
        company_id: int,
        domain: str,
        output_dir: Path
    ) -> Dict[str, Any]:
        """
        Extract data from pages.

        Args:
            pages: List of dicts with 'url' and 'text' keys
            company_id: Company ID for tracking
            domain: Domain name for logging
            output_dir: Directory to save artifacts

        Returns:
            Extracted data as dict matching schema

        Raises:
            ExtractionError: If extraction fails
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Return client name for logging (e.g., 'GLM', 'Gemini')"""
        pass

    @property
    @abstractmethod
    def token_limit(self) -> int:
        """Return token limit for this client"""
        pass

    @property
    @abstractmethod
    def split_threshold(self) -> int:
        """Return threshold at which to split requests"""
        pass


class ExtractionError(Exception):
    """Raised when extraction fails"""
    pass
