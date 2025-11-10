"""
LLM-based entity extraction using Pydantic AI.

This module provides structured entity extraction from French public tender documents
using language models. It replaces the previous GLiNER-based NER pipeline with a more
flexible and accurate LLM-based approach.
"""

import logging
from typing import Optional
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel

logger = logging.getLogger(__name__)


# Pydantic models for structured extraction
class EntitySpan(BaseModel):
    """Represents a single extracted entity with text and position information."""
    text: str = Field(description="The extracted text")
    start: int = Field(description="Start character position in the document")
    stop: int = Field(description="End character position in the document")
    confidence: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Confidence score for this extraction (0-1)"
    )


class SelectionCriteria(EntitySpan):
    """Selection criteria or requirements (critère de sélection ou exigence)."""
    pass


class ProjectDuration(EntitySpan):
    """Project duration (durée du projet)."""
    pass


class ApplicationDeadline(EntitySpan):
    """Application deadline (date limite candidature)."""
    pass


class DeliverableType(EntitySpan):
    """Type of deliverable (type de livrable)."""
    pass


class BudgetAmount(EntitySpan):
    """Price or expected budget in euros (prix ou budget prévu en euros)."""
    pass


class TenderExtraction(BaseModel):
    """Complete extraction result for a tender document."""

    selection_criteria: list[SelectionCriteria] = Field(
        default_factory=list,
        description="Selection criteria or requirements mentioned in the tender"
    )
    project_duration: list[ProjectDuration] = Field(
        default_factory=list,
        description="Project duration or timeline information"
    )
    application_deadline: list[ApplicationDeadline] = Field(
        default_factory=list,
        description="Application or submission deadlines"
    )
    deliverable_type: list[DeliverableType] = Field(
        default_factory=list,
        description="Types of deliverables expected"
    )
    budget_amount: list[BudgetAmount] = Field(
        default_factory=list,
        description="Budget amounts or price information in euros"
    )


# System prompt for the extraction agent
EXTRACTION_SYSTEM_PROMPT = """You are an expert at extracting structured information from French public tender documents.

Your task is to carefully read the tender document and extract the following information:

1. **Selection criteria or requirements** (critères de sélection ou exigences): Any requirements, qualifications, or selection criteria that applicants must meet.

2. **Project duration** (durée du projet): Information about how long the project will last, including start and end dates, duration in months/years, etc.

3. **Application deadline** (date limite de candidature): The deadline by which applications or bids must be submitted.

4. **Deliverable type** (type de livrable): What the successful bidder will need to deliver (e.g., services, goods, construction, studies, reports).

5. **Budget amount** (prix ou budget prévu): The budget, estimated cost, or price information in euros.

For each extraction:
- Provide the exact text as it appears in the document
- Provide the start and end character positions
- Extract ALL relevant mentions (there may be multiple)
- Only extract information that is explicitly stated in the document
- If a category has no information, leave the list empty

Be thorough but precise. Do not invent or infer information that isn't explicitly in the text."""


def create_extraction_agent(
    model_name: str = "claude-3-5-sonnet-20241022",
    api_key: Optional[str] = None,
) -> Agent[None, TenderExtraction]:
    """
    Create a Pydantic AI agent for tender document extraction.

    Args:
        model_name: The model to use. Can be:
            - Claude models: "claude-3-5-sonnet-20241022", "claude-3-opus-20240229", etc.
            - OpenAI models: "gpt-4o", "gpt-4-turbo", etc.
            - Azure OpenAI: "azure:deployment_name"
        api_key: Optional API key. If not provided, will use environment variables:
            - ANTHROPIC_API_KEY for Claude
            - OPENAI_API_KEY for OpenAI
            - AZURE_OPENAI_API_KEY for Azure OpenAI

    Returns:
        A configured Pydantic AI agent for extraction.
    """
    # Determine the model provider
    if model_name.startswith("azure:"):
        # Azure OpenAI format: azure:deployment_name
        deployment_name = model_name.split(":", 1)[1]
        model = OpenAIModel(
            deployment_name,
            base_url=None,  # Will be read from AZURE_OPENAI_ENDPOINT env var
            api_key=api_key,  # Will use AZURE_OPENAI_API_KEY if not provided
        )
    elif model_name.startswith("gpt"):
        # OpenAI models
        model = model_name
    else:
        # Default to the model name as-is (e.g., Claude)
        model = model_name

    agent = Agent(
        model,
        result_type=TenderExtraction,
        system_prompt=EXTRACTION_SYSTEM_PROMPT,
    )

    return agent


async def extract_entities_from_text(
    text: str,
    model_name: str = "claude-3-5-sonnet-20241022",
    api_key: Optional[str] = None,
) -> TenderExtraction:
    """
    Extract entities from tender document text using an LLM.

    Args:
        text: The tender document text to extract from.
        model_name: The model to use for extraction.
        api_key: Optional API key for the model.

    Returns:
        TenderExtraction object with all extracted entities.
    """
    agent = create_extraction_agent(model_name, api_key)

    # Run the agent
    result = await agent.run(
        f"Please extract all relevant information from this French tender document:\n\n{text}"
    )

    return result.data


def convert_extraction_to_labels(
    extraction: TenderExtraction,
    archive_id: int,
) -> list[dict]:
    """
    Convert a TenderExtraction into the label format expected by the database.

    Args:
        extraction: The TenderExtraction object.
        archive_id: The archive ID to associate labels with.

    Returns:
        List of label dictionaries in the format expected by upsert_archive_labels.
    """
    labels = []

    # Map entity types to their database label names
    entity_mapping = [
        ("selection_criteria", "critere_de_selection_ou_exigence"),
        ("project_duration", "duree_du_projet"),
        ("application_deadline", "date_limite_candidature"),
        ("deliverable_type", "type_de_livrable"),
        ("budget_amount", "prix_ou_budget_prevu_en_euros"),
    ]

    for field_name, label_name in entity_mapping:
        entities = getattr(extraction, field_name)
        for entity in entities:
            labels.append({
                "label": label_name,
                "text": entity.text,
                "start": entity.start,
                "stop": entity.stop,
                "score": entity.confidence if entity.confidence is not None else 1.0,
            })

    return labels
