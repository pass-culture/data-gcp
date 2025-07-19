import asyncio
import logging
from datetime import datetime
from typing import List, Dict

import pandas as pd

from client import AsyncBrevoClient
from config import Config

logger = logging.getLogger(__name__)



class TransactionalExtractor:
    """Handles extraction of transactional email data"""
    def __init__(self, config: Config, start_date: str, end_date: str):
        self.config = config
        self.client = AsyncBrevoClient(config)
        self.start_date = start_date
        self.end_date = end_date
    
    async def get_active_templates(self) -> List[int]:
        """Get all active template IDs"""
        try:
            data = await self.client.request("GET", "smtp/templates", params={"templateStatus": "true"})
            templates = data.get("templates", [])
            
            # In prod, get all templates; in dev, just get first one for testing
            if self.config.environment == "prod":
                # TODO: Handle pagination if more than 50 templates
                return [template["id"] for template in templates]
            else:
                return [templates[0]["id"]] if templates else []
                
        except Exception as e:
            logger.error(f"Failed to get templates: {e}")
            return []
    
    async def get_events_for_template(self, template_id: int, event_type: str) -> List[Dict]:
        """Get all events for a specific template and event type"""
        all_events = []
        offset = 0
        
        while True:
            try:
                data = await self.client.request(
                    "GET", 
                    "smtp/statistics/events",
                    params={
                        "templateId": template_id,
                        "event": event_type,
                        "startDate": self.start_date,
                        "endDate": self.end_date,
                        "offset": offset,
                        "limit": 2500
                    }
                )
                
                events = data.get("events", [])
                all_events.extend(events)
                
                logger.info(f"Template {template_id} {event_type}: fetched {len(events)} events (total: {len(all_events)})")
                
                # Stop if we got less than a full page
                if len(events) < 2500:
                    break
                    
                # Continue to next page
                offset += 2500
                
                # In dev mode, only get first page
                if self.config.environment != "prod":
                    break
                    
            except Exception as e:
                logger.error(f"Failed to get events for template {template_id}, {event_type}: {e}")
                break
                
        return all_events
    
    async def get_all_events(self, template_ids: List[int]) -> List[Dict]:
        """Get all events for all templates concurrently"""
        tasks = []
        
        # Create tasks for all template/event combinations
        for template_id in template_ids:
            for event_type in ["delivered", "opened", "unsubscribed"]:
                task = self.get_events_for_template(template_id, event_type)
                tasks.append(task)
        
        # Execute all tasks concurrently
        logger.info(f"Fetching events for {len(template_ids)} templates...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Flatten results and handle errors
        all_events = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
            else:
                all_events.extend(result)
                
        logger.info(f"Total events collected: {len(all_events)}")
        return all_events
    
    def transform_to_dataframe(self, events: List[Dict]) -> pd.DataFrame:
        """Transform raw events into aggregated DataFrame"""
        if not events:
            # Return empty dataframe with correct schema
            return pd.DataFrame(columns=[
                "template", "tag", "email", "event_date", 
                "delivered_count", "opened_count", "unsubscribed_count"
            ])
        
        # Create DataFrame from events
        df = pd.DataFrame(events)
        
        # Rename columns to match your schema
        df = df.rename(columns={
            "templateId": "template",
            "date": "event_date"
        })
        
        # Convert date to proper format
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
        
        # Group by template, tag, email, and date, then count events
        grouped = df.groupby(
            ["template", "tag", "email", "event_date", "event"]
        ).size().reset_index(name="count")
        
        # Pivot to get event counts as columns
        pivoted = grouped.pivot_table(
            index=["template", "tag", "email", "event_date"],
            columns="event",
            values="count",
            fill_value=0
        ).reset_index()
        
        # Rename columns to match schema
        pivoted = pivoted.rename(columns={
            "delivered": "delivered_count",
            "opened": "opened_count",
            "unsubscribed": "unsubscribed_count"
        })
        
        # Ensure all required columns exist
        for col in ["delivered_count", "opened_count", "unsubscribed_count"]:
            if col not in pivoted.columns:
                pivoted[col] = 0
                
        # Add target column
        pivoted["target"] = "pro" if "pro" in self.config.api_key else "native"
        
        return pivoted
    
    async def extract(self) -> pd.DataFrame:
        """Main extraction method"""
        async with self.client:
            # Get active templates
            template_ids = await self.get_active_templates()
            logger.info(f"Found {len(template_ids)} active templates")
            
            if not template_ids:
                logger.warning("No active templates found")
                return pd.DataFrame()
            
            # Get all events
            events = await self.get_all_events(template_ids)
            
            # Transform to DataFrame
            df = self.transform_to_dataframe(events)
            logger.info(f"Transformed {len(events)} events into {len(df)} aggregated rows")
            
            return df


class NewsletterExtractor:
    """Handles extraction of newsletter campaign data"""
    pass