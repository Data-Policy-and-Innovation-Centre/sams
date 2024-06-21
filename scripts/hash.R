#hashing address column of sams DPIC Data Skill 

#install and load packages
install.packages("openxlsx")
install.packages("digest")
library(digest)
library(openxlsx)

#import the sams dataset
DPIC_Data_Skill <- read_excel("C:/Users/amita/Box/Amitav/Skills/SAMS/data/raw/DPIC Data Skill.xlsm")

#hash the address using digest function
DPIC_Data_Skill$Hashed_Address <- sapply(DPIC_Data_Skill$Address, digest, algo = "sha256")

#remove the address column
DPIC_Data_Skill$Address <- NULL

#export hashed data as excel
write.xlsx(DPIC_Data_Skill, "C:/Users/amita/Box/Amitav/Skills/SAMS/data/processed/Hashed_Data_Skill.xlsx")  
