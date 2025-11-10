#!/usr/bin/env python3
"""
Quick test script for the LLM extraction pipeline.
"""

import os
import asyncio
from openplace.tasks.extract.llm import extract_entities_from_text

# Sample French tender document text for testing
SAMPLE_TEXT = """
AVIS DE MARCHE PUBLIC

Objet du marché: Fourniture et installation de matériel informatique

SECTION I: POUVOIR ADJUDICATEUR
Organisation: Mairie de Paris
Département: Paris (75)

SECTION II: DESCRIPTION DU MARCHÉ

II.1) Intitulé: Fourniture de 50 ordinateurs portables et leurs accessoires

II.2) Description: Le présent marché a pour objet la fourniture et l'installation de 50 ordinateurs
portables destinés aux services administratifs de la mairie. Les ordinateurs devront être équipés
de Windows 11 Professionnel et d'une suite bureautique complète.

II.3) Durée du marché: 12 mois à compter de la notification

II.4) Critères d'attribution:
- Prix: 40%
- Valeur technique: 30%
- Qualité du service après-vente: 20%
- Délai de livraison: 10%

Les candidats devront justifier d'au moins 3 ans d'expérience dans la fourniture de matériel
informatique pour des collectivités publiques.

SECTION III: CONDITIONS DE PARTICIPATION

III.1) Exigences techniques et professionnelles:
- Certification ISO 9001
- Références de marchés similaires
- Capacité financière minimum de 100 000 euros

SECTION IV: PROCÉDURE

IV.1) Type de procédure: Appel d'offres ouvert

IV.2) Date limite de réception des offres: 15 mars 2025, 12h00

IV.3) Budget prévisionnel: 85 000 euros TTC

SECTION V: RENSEIGNEMENTS COMPLÉMENTAIRES

Les candidats devront fournir les documents suivants:
- DC1 (lettre de candidature)
- DC2 (déclaration du candidat)
- Attestations fiscales et sociales
- Moyens humains et techniques

Les livrables attendus comprennent:
- Les ordinateurs portables conformes aux spécifications
- Les logiciels préinstallés et configurés
- La documentation technique
- La formation des utilisateurs (1 journée)
"""


async def test_extraction():
    """Test the extraction with sample text."""
    print("Testing LLM extraction with sample French tender document...")
    print("-" * 80)

    try:
        extraction = await extract_entities_from_text(
            SAMPLE_TEXT,
            model_name="claude-3-5-sonnet-20241022",
        )

        print("\n✓ Extraction completed successfully!\n")

        print(f"Selection Criteria: {len(extraction.selection_criteria)} found")
        for item in extraction.selection_criteria:
            print(f"  - {item.text[:100]}... (pos: {item.start}-{item.stop})")

        print(f"\nProject Duration: {len(extraction.project_duration)} found")
        for item in extraction.project_duration:
            print(f"  - {item.text[:100]}... (pos: {item.start}-{item.stop})")

        print(f"\nApplication Deadline: {len(extraction.application_deadline)} found")
        for item in extraction.application_deadline:
            print(f"  - {item.text[:100]}... (pos: {item.start}-{item.stop})")

        print(f"\nDeliverable Type: {len(extraction.deliverable_type)} found")
        for item in extraction.deliverable_type:
            print(f"  - {item.text[:100]}... (pos: {item.start}-{item.stop})")

        print(f"\nBudget Amount: {len(extraction.budget_amount)} found")
        for item in extraction.budget_amount:
            print(f"  - {item.text[:100]}... (pos: {item.start}-{item.stop})")

        print("\n" + "=" * 80)
        print("Test completed successfully!")

    except Exception as e:
        print(f"\n✗ Error during extraction: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Check for API key
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("Warning: ANTHROPIC_API_KEY not set. Set it with:")
        print("  export ANTHROPIC_API_KEY='your-key-here'")
        print("\nSkipping test...")
    else:
        asyncio.run(test_extraction())
