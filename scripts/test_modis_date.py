import asyncio
import sys
sys.path.insert(0, 'E:\\SDA project')

from backend.pipelines.modis import process_modis_for_land_day

async def main():
    for d in ['2026-02-24','2026-02-23','2026-02-22','2026-02-21','2026-02-20']:
        print('\n=== Testing MODIS for', d, '===')
        try:
            res = await process_modis_for_land_day(15, d)
            print(res)
        except Exception as e:
            print('Error:', e)

if __name__ == '__main__':
    asyncio.run(main())
