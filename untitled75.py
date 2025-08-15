#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  7 19:04:42 2025

@author: guckele
"""

import pandas as pd

df = pd.read_excel("data/DT2A_FY2023.xlsx", sheet_name="Total Funding Table")
print(df.columns.tolist())