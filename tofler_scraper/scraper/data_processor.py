"""Data processing and segregation"""
import config

class DataProcessor:
    @staticmethod
    def is_director(designation):
        """Check if designation contains 'director' (case-insensitive)"""
        return "director" in designation.lower() if designation else False
    
    @staticmethod
    def process_person_data(company_id, person, directorships, original_company_name):
        """Process scraped data for a single person
        
        Args:
            company_id: Original company ID
            person: Dict with person info (name, designation, din, etc.)
            directorships: List of directorship dicts
            original_company_name: Name of the company being scraped (to filter out)
            
        Returns:
            tuple: (directors_data, management_data)
        """
        directors_data = []
        management_data = []
        
        person_name = person.get("name", "")
        person_designation = person.get("designation", "")
        
        # Filter out original company from directorships
        filtered_directorships = [
            d for d in directorships
            if not DataProcessor._is_same_company(
                d.get("name", ""),
                original_company_name
            )
        ]
        
        if DataProcessor.is_director(person_designation):
            # Add to directors sheet - one row per directorship
            for directorship in filtered_directorships:
                directors_data.append({
                    "company_id": company_id,
                    "person_name": person_name,
                    "designation": person_designation,
                    "related_company": directorship.get("name", ""),
                    "industry": directorship.get("industry", ""),
                    "status": directorship.get("status", ""),
                    "Designation_in_other_company": directorship.get("designation", ""),
                    "contact": ""
                })
        else:
            # Add to management sheet
            management_data.append({
                "id": f"MG{company_id}",
                "company_id": company_id,
                "name": person_name,
                "designation": person_designation,
                "contact": ""
            })
        
        return directors_data, management_data
    
    @staticmethod
    def _is_same_company(name1, name2):
        """Check if two company names are the same (fuzzy match)"""
        # Normalize names
        def normalize(name):
            return name.lower().strip().replace("private limited", "").replace("pvt ltd", "").replace("pvt. ltd.", "").strip()
        
        return normalize(name1) == normalize(name2)
