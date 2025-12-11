#!/usr/bin/env python3
"""
Manage pipeline schedules
"""

import yaml
import json
from datetime import datetime
import os

class ScheduleManager:
    """Manage GitHub Actions schedules"""
    
    def __init__(self, workflow_file='.github/workflows/data-pipeline.yml'):
        self.workflow_file = workflow_file
        
    def read_schedule(self):
        """Read current schedule from workflow file"""
        with open(self.workflow_file, 'r') as f:
            workflow = yaml.safe_load(f)
        
        schedules = workflow.get('on', {}).get('schedule', [])
        return schedules
    
    def update_schedule(self, new_schedules):
        """Update schedule in workflow file"""
        with open(self.workflow_file, 'r') as f:
            workflow = yaml.safe_load(f)
        
        # Update schedules
        if 'schedule' in workflow.get('on', {}):
            workflow['on']['schedule'] = new_schedules
        
        # Write back
        with open(self.workflow_file, 'w') as f:
            yaml.dump(workflow, f, default_flow_style=False)
        
        print(f"Updated schedule in {self.workflow_file}")
        
    def add_daily_schedule(self, hour_utc, minute_utc=0, weekdays='1-5'):
        """Add a daily schedule"""
        cron = f'{minute_utc} {hour_utc} * * {weekdays}'
        
        schedules = self.read_schedule()
        schedules.append({'cron': cron})
        self.update_schedule(schedules)
        
        return cron
    
    def remove_all_schedules(self):
        """Remove all schedules"""
        self.update_schedule([])
        print("All schedules removed")
    
    def show_schedules(self):
        """Display current schedules"""
        schedules = self.read_schedule()
        
        print("Current Schedules:")
        print("-" * 40)
        
        for i, schedule in enumerate(schedules, 1):
            cron = schedule.get('cron', '')
            print(f"{i}. {cron}")
            self._explain_cron(cron)
            print()
    
    def _explain_cron(self, cron_str):
        """Explain cron syntax"""
        try:
            minute, hour, day, month, weekday = cron_str.split()
            
            days_map = {
                '1-5': 'Mon-Fri',
                '0-6': 'Sun-Sat',
                '0': 'Sunday',
                '1': 'Monday',
                '2': 'Tuesday',
                '3': 'Wednesday',
                '4': 'Thursday',
                '5': 'Friday',
                '6': 'Saturday'
            }
            
            weekday_desc = days_map.get(weekday, weekday)
            
            # Convert to Eastern Time
            hour_et = (int(hour) - 5) % 24  # UTC-5 for EST
            if hour_et < 0:
                hour_et += 24
            
            print(f"   Runs at: {hour_et:02d}:{minute} Eastern Time")
            print(f"   Days: {weekday_desc}")
            
        except:
            print(f"   Could not parse: {cron_str}")

def main():
    """Command line interface"""
    import argparse
    
    manager = ScheduleManager()
    
    parser = argparse.ArgumentParser(description='Manage pipeline schedules')
    parser.add_argument('--show', action='store_true', help='Show current schedules')
    parser.add_argument('--add', help='Add schedule (format: "HH:MM weekdays")')
    parser.add_argument('--remove-all', action='store_true', help='Remove all schedules')
    parser.add_argument('--convert', help='Convert local time to cron (format: "HH:MM TIMEZONE")')
    
    args = parser.parse_args()
    
    if args.show:
        manager.show_schedules()
    
    elif args.add:
        try:
            time_str, weekdays = args.add.split()
            hour, minute = time_str.split(':')
            
            # Convert to UTC (assuming Eastern Time input)
            hour_utc = (int(hour) + 5) % 24  # ET to UTC
            
            cron = manager.add_daily_schedule(hour_utc, int(minute), weekdays)
            print(f"Added schedule: {cron}")
            
        except ValueError:
            print("Invalid format. Use: --add '14:30 1-5'")
    
    elif args.remove_all:
        confirm = input("Remove all schedules? (yes/no): ")
        if confirm.lower() == 'yes':
            manager.remove_all_schedules()
    
    elif args.convert:
        # Simple timezone conversion helper
        time_str, tz = args.convert.split()
        hour, minute = time_str.split(':')
        
        conversions = {
            'EST': 5,  # UTC-5
            'EDT': 4,  # UTC-4
            'CST': 6,  # UTC-6
            'PST': 8,  # UTC-8
            'UTC': 0
        }
        
        if tz.upper() in conversions:
            hour_utc = (int(hour) + conversions[tz.upper()]) % 24
            print(f"{time_str} {tz} = {hour_utc:02d}:{minute} UTC")
        else:
            print(f"Unknown timezone: {tz}")
    
    else:
        manager.show_schedules()

if __name__ == "__main__":
    main()
