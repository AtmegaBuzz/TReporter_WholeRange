from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import GetMessagesRequest
from telethon.tl.types import PeerChat
from telethon.sessions import StringSession
# from webserver import keep_alive

import gspread
import config
from time import sleep

# keep_alive()
# url = config.spreadsheet_url
gc = gspread.service_account(filename=config.spread_user_cred_json_file_addr)
gsheet = gc.open_by_url(config.fetch_spreadsheet_url)
worksheet = gsheet.worksheets()[0]

session_str = "1BVtsOLABu3Fs9YU26xwZFDXJbPkAY43kYuxU2cLNpVF9K1ZQIHbvo-oWFstTcVb6MQJeJhu0LBP_WPgdHJoUZQGUOX5wpt13nRlpxqOFBp2lnubbenDJb5YKj_RoPlsDB9jB--SaCIvUr2j7QcUAU2XCov28koOXAqK0mZGvOA0owBnD3FCcTC2hwfJnkT907nxpSAikppZPJo_1pjPS2iacq_ij_5ANvDBgiFozy1LUsS30yndx42Empt-Qz6M64l5sKQnkzg4g_7THHije4HNnN-LpNZSSqOsPSQy3CyBhfr8pbloxJGBYaXnkr6DGWE6oCqjDLqwjFOzK_4mrFdtuVanWQUo="

client = TelegramClient(StringSession(session_str),config.api_id,config.api_hash)


    
async def filter_and_submit_report(message,out_worksheet):
    try:
        username_entity = await client.get_entity(message.from_id.user_id)
        message_posted_by_username = username_entity.username
        if(message_posted_by_username is not None):
            
            message_text = message.message
            message_channel = await client.get_entity(message.peer_id.channel_id)
            message_date = message.date.date()
            to_append = [f"{message_date}",message_posted_by_username,message_channel.username,message_text] 
            out_worksheet.append_row(to_append)
            sleep(1)
                            
                       
               
        else:
            print("none passed")
                
    except Exception as e:
        print(e,"no worries, its a log of error")
        

async def t_scrapper(starting_link,ending_link,out_worksheet,):
    print(starting_link,ending_link)
    channel_name,starting_message_id = starting_link.split("/")[-2],starting_link.split("/")[-1]
    ending_message_id = ending_link.split("/")[-1]

    await client(JoinChannelRequest(channel_name))
    channel_entity = await client.get_entity(channel_name)

    messages = client.iter_messages(
        channel_entity,
        limit=5000,
        min_id=int(starting_message_id),
        max_id=int(ending_message_id),
        reverse=True
    )
    count = 0

    async for message in messages:
        print(count)
        count+=1
        await filter_and_submit_report(message,out_worksheet)

def get_links(url):
    gc = gspread.service_account(filename=config.spread_user_cred_json_file_addr)
    gsheet = gc.open_by_url(url)
    in_worksheet = gsheet.sheet1
    out_worksheet = gsheet.worksheet('Sheet2')
    return in_worksheet.col_values(1) , in_worksheet.col_values(2),out_worksheet

    

async def main():
    from config import starting_row
    starting_links,ending_links,out_worksheet = get_links(config.fetch_spreadsheet_url)
 
    
    for i in range(starting_row,len(starting_links)):
        try:
            await t_scrapper(starting_links[i], ending_links[i],out_worksheet)
        except Exception as e:
            print(e,"exception handeled")


with client:
    client.loop.run_until_complete(main())

