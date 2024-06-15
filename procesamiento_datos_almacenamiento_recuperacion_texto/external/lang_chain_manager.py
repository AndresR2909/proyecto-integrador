from langchain_community.document_loaders import DataFrameLoader
from langchain_text_splitters import CharacterTextSplitter,RecursiveCharacterTextSplitter,TokenTextSplitter
from langchain_pinecone import PineconeVectorStore
import pandas as pd
from langchain_openai import AzureOpenAIEmbeddings
import os
from langchain.docstore.document import Document
from langchain.chains import MapReduceDocumentsChain, ReduceDocumentsChain
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.chains.llm import LLMChain
from langchain_core.prompts import PromptTemplate
from langchain.chains.summarize import load_summarize_chain
import tiktoken

OPENAI_ENDPOINT = os.environ["AZURE_OPENAI_ENDPOINT"]
OPENAI_API_KEY = os.environ["AZURE_OPENAI_API_KEY"] 
EMBEDDING_DEPLOYMENT = os.environ["AZURE_EMBEDDING_DEPLOYMENT"] 
_ = os.environ.get('PINECONE_API_KEY')

class LangChainManager:

    def __init__(self) -> None:
        self.embedding_deployment_name= EMBEDDING_DEPLOYMENT
        self.embedding = AzureOpenAIEmbeddings(azure_deployment=EMBEDDING_DEPLOYMENT)

    def load_df_document(self, df:pd.DataFrame,text_content_column:str="text"):
        """load dataframe into documents"""
        loader = DataFrameLoader(df, page_content_column=text_content_column)
        
        if len(df)>10000:
            temp_docs = []
            for d in loader.lazy_load():
                temp_docs.extend(d)
            
            docs= temp_docs
        else:
            docs = loader.load()
        return docs
    
    def split_document(self, docs, chunk_size:int=2000,chunk_overlap:int=200, token:bool =True, character:bool=False):
        """Split documents"""
        if token:
           text_splitter = self._token_text_splitter(chunk_size,
                                                    chunk_overlap,
                                                    model_name = 'text-embedding-ada-002')

        elif character:
            text_splitter = self._character_text_splitter(chunk_size,
                                                        chunk_overlap,
                                                        sep="\n")
        else:
            text_splitter = self._recursive_character_text_splitter(chunk_size,
                                                    chunk_overlap,
                                                    sep=["\n\n", "\n", "\. ", " "])
        splits = text_splitter.split_documents(docs)
        return splits

    def embed_store_documents(self,index_name:str,docs_splits):
        vectordb = PineconeVectorStore.from_documents(
                                        documents=docs_splits,
                                        embedding=self.embedding,
                                        index_name=index_name)
        return vectordb
        
    def document_search(self,index_name:str,query:str,n_documents:int=4,metadata_filter:dict=None):
        vectordb = PineconeVectorStore(index_name=index_name,
                                       embedding=self.embedding)  
        docs = vectordb.similarity_search(query,
                                        k=n_documents,
                                        filter=metadata_filter
                                    ) 
        return docs
    
    def add_documents_vectostore(self, index_name:str,documents:list,ids:list):
        vectordb = PineconeVectorStore(index_name=index_name,
                                       embedding=self.embedding)  
        ids_list = vectordb.add_documents(documents,ids=ids)
        
        return ids_list
    
    def add_ids_to_documents(self,split_docs,id_column:str="video_id"):
        tokenizer = tiktoken.get_encoding("cl100k_base")
        documents=[]
        id_list=[]
        last_column_id = ""
        for d in split_docs:
            column_id =d.metadata[id_column]
            if column_id == last_column_id:
                split=split+1
            else:
                split = 0
            d.metadata['split_tokens'] = total_tokens=len(tokenizer.encode(d.page_content))
            d.metadata['part'] = split
            documents.append(Document(ids=f'{column_id}-{split}', page_content=d.page_content, metadata=d.metadata))
            id_list.append(f'{column_id}-{split}')
            
            last_column_id=column_id

        return documents,id_list
    def resume_documents_stuff(self,documents:list,llm):
        chain = load_summarize_chain(llm, chain_type="stuff")
        try:
            result = chain.invoke(documents)
        except Exception as e:
            msn = f"error: {e}"
            print(msn)
            result = {'output_text':msn}
        return result
    def _format_docs(self,docs):
        return "\n\n".join(doc.page_content for doc in docs)
    
    def custom_summary_documents(self,documents:list,llm):

        template = """ Eres un experto de trading y analsis de mercado.Tu trabajo consiste en elaborar un informe con los topicos mas importantes del siguiente texto:
        ------------
        {context}
        ------------
        Comience el resumen final con una "Introducción" que ofrezca una visión general del tema seguido
        por los puntos más importantes ("Bullet Points"). Termina el resumen con una conclusión.

        Respuesta:"""
        prompt = PromptTemplate.from_template(template)
        chain = self._format_docs | prompt | llm
        response= chain.invoke(documents)
        
        return response.content
    
    def map_reduce_custom_summary_documents(self,documents:str,llm):

        template = """ Eres un experto de trading y analsis de mercado.
        Tu trabajo consiste en elaborar un informe final uniendo los siguientes textos:
        ------------
        {context}
        ------------
        Comience el resumen final con una "Introducción" que ofrezca una visión general de los temas seguido
        por los puntos más importantes ("Bullet Points") de cada tema. Termina el resumen con una conclusión de cada tema.

        Respuesta:"""
        prompt = PromptTemplate.from_template(template)
        chain =  prompt | llm
        response= chain.invoke(documents)
        
        return response.content
        


    
    def resume_documents_refine(self,documents:list,llm):
        prompt_template = """Escribe un resumen conciso del siguiente texto extrayendo la información clave:
        Texto: {text}
        Resumen:"""
        #prompt = PromptTemplate.from_template(prompt_template)
        prompt = PromptTemplate(template=prompt_template, input_variables=['text'])

        refine_template = '''
            Tu trabajo consiste en elaborar un resumen final.
            He proporcionado un resumen existente hasta cierto punto: {existing_answer}.
            Por favor, perfeccione el resumen existente con algo más de contexto a continuación.
            ------------
            {text}
            ------------
            Comience el resumen final con una "Introducción" que ofrezca una visión general del tema seguido
            por los puntos más importantes ("Bullet Points"). Termina el resumen con una conclusión.
        '''
        #refine_prompt = PromptTemplate.from_template(refine_template)
        refine_prompt = PromptTemplate(template=refine_template,
                                        input_variables=['existing_answer', 'text'])
        chain = load_summarize_chain(
            llm=llm,
            chain_type="refine",
            question_prompt=prompt,
            refine_prompt=refine_prompt,
            return_intermediate_steps=True,
            input_key="input_documents",
            output_key="output_text",
        )
        try:
            result = chain.invoke({"input_documents": documents}, return_only_outputs=True)
        except Exception as e:
            msn = f"error: {e}"
            print(msn)
            try:
                result = chain.invoke({"input_documents": documents}, return_only_outputs=True)
            except Exception as e:
                msn = f"error: {e}"
                print(msn)
                result = {'output_text':msn}
 
        return result
        
    def resume_documents_with_map_reduce(self,documents:list,llm):

        map_template  = """Escribe un resumen conciso de los siguientes documentos,  extrayendo la información clave:
        Texto: {docs}
        Resumen:"""
        map_prompt = PromptTemplate.from_template(map_template)
        map_chain = LLMChain(llm=llm, prompt=map_prompt)


        reduce_template = '''
            Tu trabajo consiste en elaborar un resumen final.
            He proporcionado una lista de  resumenes : {docs}.
            Por favor, una los resumenes en un resumen final.
            Comience el resumen final con una "Introducción" que ofrezca una visión general del tema seguido
            por los puntos más importantes ("Bullet Points"). Termina el resumen con una conclusión.
        '''
        reduce_prompt = PromptTemplate.from_template(reduce_template)

        # Run chain
        reduce_chain = LLMChain(llm=llm, prompt=reduce_prompt)

        # Takes a list of documents, combines them into a single string, and passes this to an LLMChain
        combine_documents_chain = StuffDocumentsChain(
            llm_chain=reduce_chain, document_variable_name="docs"
        )

        # Combines and iteratively reduces the mapped documents
        reduce_documents_chain = ReduceDocumentsChain(
            # This is final chain that is called.
            combine_documents_chain=combine_documents_chain,
            # If documents exceed context for `StuffDocumentsChain`
            collapse_documents_chain=combine_documents_chain,
            # The maximum number of tokens to group documents into.
            token_max=4000,
            )
        # Combining documents by mapping a chain over them, then combining results
        map_reduce_chain = MapReduceDocumentsChain(
            # Map chain
            llm_chain=map_chain,
            # Reduce chain
            reduce_documents_chain=reduce_documents_chain,
            # The variable name in the llm_chain to put the documents in
            document_variable_name="docs",
            # Return the results of the map steps in the output
            return_intermediate_steps=True,
        )
        try:
            result = map_reduce_chain.invoke(documents)
        except Exception as e:
            msn = f"error: {e}"
            print(msn)
            try:
                result = map_reduce_chain.invoke(documents)
            except Exception as e:
                msn = f"error: {e}"
                print(msn)
                result = {'output_text':msn}
        return result

    def count_tokens_from_string(self,text:str):
        tokenizer = tiktoken.get_encoding("cl100k_base")
        total_tokens = len(tokenizer.encode(text))
        return total_tokens
    
    def count_tokens(self,documents:list):
        total_caracteres = 0
        total_tokens =0
        tokenizer = tiktoken.get_encoding("cl100k_base")

        for doc in documents:
            total_caracteres += len(doc.page_content)
            total_tokens += len(tokenizer.encode(doc.page_content))
        return total_tokens
    
    def _character_text_splitter(self,chunk_size:int,chunk_overlap:int,sep=" "):
        text_splitter = CharacterTextSplitter(
        separator=sep,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len)
        
        return text_splitter
    
    def _recursive_character_text_splitter(self,chunk_size:int,chunk_overlap:int,sep=["\n\n", "\n", "\. ", " "]):
        text_splitter = RecursiveCharacterTextSplitter(separators=sep,
                                                    chunk_size=chunk_size,
                                                    chunk_overlap=chunk_overlap)
        
        return text_splitter
    
    def _token_text_splitter(self,chunk_size:int,chunk_overlap:int, model_name:str):
        text_splitter = TokenTextSplitter(encoding_name='cl100k_base',
                                          model_name= model_name,
                                        chunk_size=chunk_size, 
                                        chunk_overlap=chunk_overlap)
        
        return text_splitter
    
    def _character_text_splitter_v2(self,chunk_size:int,chunk_overlap:int):
        CharacterTextSplitter.from_tiktoken_encoder(encoding_name='cl100k_base',
                                                    chunk_size=chunk_size, 
                                                    chunk_overlap=chunk_overlap
                                                    )
