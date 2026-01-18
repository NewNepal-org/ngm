from scrapy.pipelines.files import FilesPipeline

class KanunPatrikaPipeline(FilesPipeline):
    """Pipeline for downloading Kanun Patrika PDF files with custom naming."""
    
    def file_path(self, request, response=None, info=None, *, item=None):
        """Generate custom file path based on metadata."""
        metadata = item.get('metadata', {})
        file_id = request.url.split("/")[-1].replace(".pdf", "")
        
        if metadata:
            year = metadata.get('year', '')
            month = metadata.get('month', '')
            volume = metadata.get('volume', '')
            issue = metadata.get('issue', '')
            return f"{year} {month} भाग {volume} अंक {issue} - {file_id}.pdf"
        
        return f"{file_id}.pdf"

    def item_completed(self, results, item, info):
        """Log download results."""
        for ok, result in results:
            if ok:
                file_path = result['path']
                info.spider.logger.info(f"Downloaded: {file_path}")
            else:
                info.spider.logger.error(f"Failed: {item['file_urls'][0]}")
        return item