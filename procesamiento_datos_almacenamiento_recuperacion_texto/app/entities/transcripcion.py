from typing import Optional
from pydantic import BaseModel, Field


class TranscripLanding(BaseModel):
    chanel_name:str = Field(..., title="Channel Name from youtube")
    chanel_id:str = Field(..., title="Channel Id from youtube")
    chanel_url:str = Field(..., title="Channel Url from youtube")
    video_id:str = Field(..., title="Video Id from youtube")
    title:str = Field(..., title="Video title from youtube")
    url:str = Field(..., title="Video Url from youtube")
    keywords:str
    publish_date:str = Field(..., title="Video publish date from youtube")
    relativeDateText:str
    total_length:str
    total_views:str
    video_rating:str
    description:str
    caption_text_es:str
    
class TranscripRaw(BaseModel):
    chanel_name:str = Field(..., title="Channel Name from youtube")
    chanel_id:str = Field(..., title="Channel Id from youtube")
    chanel_url:str = Field(..., title="Channel Url from youtube")
    video_id:str = Field(..., title="Video Id from youtube")
    title:str = Field(..., title="Video title from youtube")
    url:str = Field(..., title="Video Url from youtube")
    publish_date:str = Field(..., title="Video publish date from youtube")
    total_length:str
    total_views:str
    video_rating:str
    description:str
    caption_text_es:str
    createDate: str = Field(..., title="CreateDate")
    lastUpdateDate: str = Field(..., title="LastUpdateDate")

class ProcessedTranscrip(BaseModel):
    id_video: str = Field(..., title="Id")
    texto: str = Field(..., title="Name")
    description: Optional[str] = Field(title="Description")
    createDate: str = Field(..., title="CreateDate")
    lastUpdateDate: str = Field(..., title="LastUpdateDate")



    