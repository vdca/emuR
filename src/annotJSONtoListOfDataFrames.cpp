#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
List annotJSONtoListOfDataFrames(List annot) {
  List levels = annot["levels"];
  List links = annot["links"];
  
  ///////////////////////////////////
  // create links data.frame
  NumericVector fromIdCol(links.size());
  NumericVector toIdCol(links.size());
  
  for(int i=0; i<links.size(); i++) {
    List l =  links[i];
    fromIdCol[i] = l["fromID"];
    toIdCol[i] = l["toID"];
  }
  
  DataFrame linksDF = DataFrame::create(Named("fromID")=fromIdCol,
                                        Named("toID")=toIdCol);
  
  /////////////////////////////
  // create items and labels data.frames
  
  // get number of items in list and number of labels
  int itemCounter = 0;
  int labelCounter = 0;
  
  for(int i=0; i<levels.size(); i++) {
    List l = levels[i];
    List items = l["items"];
    List firstItem = items[0];
    List firstItemLabels = firstItem["labels"];
    labelCounter+= items.size() * firstItemLabels.size();
    itemCounter += items.size();
  }
  
  // init vectors of items data.frame
  NumericVector items_itemIdCol(itemCounter);
  CharacterVector items_levelCol(itemCounter);
  CharacterVector items_typeCol(itemCounter);
  NumericVector items_seqIdxCol(itemCounter);
  NumericVector items_sampleRateCol(itemCounter);
  NumericVector items_samplePointCol(itemCounter);
  NumericVector items_sampleStartCol(itemCounter);
  NumericVector items_sampleDurCol(itemCounter);
  
  // init vectors of labels data.frame
  NumericVector labels_itemIdCol(labelCounter);
  NumericVector labels_labelIdxCol(labelCounter);
  CharacterVector labels_nameCol(labelCounter);
  CharacterVector labels_labelCol(labelCounter);
  
  int curItemNumber = 0;
  int curLabelNumber = 0;
  NumericVector sampleRateVec = annot["sampleRate"];
  for(int i=0; i<levels.size(); i++) {
    List l = levels[i];
    CharacterVector lNameVec = l["name"];
    CharacterVector lTypeVec = l["type"];
    List items = l["items"];
    for(int j=0; j<items.size(); j++) {
      List curItem = items[j];
      List labels = curItem["labels"];
      items_itemIdCol[curItemNumber] = curItem["id"];
      items_levelCol[curItemNumber] = lNameVec[0];
      items_typeCol[curItemNumber] = lTypeVec[0];
      items_seqIdxCol[curItemNumber] = j;
      items_sampleRateCol[curItemNumber] = sampleRateVec[0];
      
      if(lTypeVec[0] == "ITEM"){
        items_samplePointCol[curItemNumber] = NA_REAL;
        items_sampleStartCol[curItemNumber] = NA_REAL;
        items_sampleDurCol[curItemNumber] = NA_REAL;
      }else if(lTypeVec[0] == "SEGMENT"){
        items_samplePointCol[curItemNumber] = NA_REAL;
        items_sampleStartCol[curItemNumber] = curItem["sampleStart"];
        items_sampleDurCol[curItemNumber] = curItem["sampleDur"];
      }else if(lTypeVec[0] == "EVENT"){
        items_samplePointCol[curItemNumber] = curItem["samplePoint"];
        items_sampleStartCol[curItemNumber] = NA_REAL;
        items_sampleDurCol[curItemNumber] = NA_REAL;
      }
      for(int k=0; k<labels.size(); k++) {
        List curLabel = labels[k];
        CharacterVector curLabelNameVec = curLabel["name"];
        CharacterVector curLabelValueVec = curLabel["value"];
        
        labels_itemIdCol[curLabelNumber] = curItem["id"];
        labels_labelIdxCol[curLabelNumber] = k;
        labels_nameCol[curLabelNumber] = curLabelNameVec[0];
        labels_labelCol[curLabelNumber] = curLabelValueVec[0];
        
        curLabelNumber += 1;
      }
      
      curItemNumber += 1;
    }
  }
  
  DataFrame itemsDF = DataFrame::create(Named("itemID")=items_itemIdCol,
                                        Named("level")=items_levelCol,
                                        Named("type")=items_typeCol,
                                        Named("seqIdx")=items_seqIdxCol,
                                        Named("sampleRate")=items_sampleRateCol,
                                        Named("samplePoint")=items_samplePointCol,
                                        Named("sampleStart")=items_sampleStartCol,
                                        Named("sampleDur")=items_sampleDurCol);
  
  DataFrame labelsDF = DataFrame::create(Named("itemID")=labels_itemIdCol,
                                         Named("labelIdx")=labels_labelIdxCol,
                                         Named("name")=labels_nameCol,
                                         Named("label")=labels_labelCol);
  
  
  List res = List::create(Named("items")=itemsDF,
                          Named("labels")=labelsDF,
                          Named("links")=linksDF);
  
  return res;
}
